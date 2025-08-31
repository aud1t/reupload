package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

type Row []interface{}

type Database interface {
	io.Closer
	GetMaxID(ctx context.Context) (uint64, error)
	LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)
	SaveRows(ctx context.Context, rows []Row) error
}

// Это функция где-то реализована
func Connect(ctx context.Context, dsn string) (Database, error) {
	return nil, nil
}

const batchSize uint64 = 10_000

const (
	numWorkers = 10
	maxRetries = 3
	retryDelay = 2 * time.Second
)

type batch struct {
	start uint64
	end   uint64
}

func batcher(startID, endID, batchSize uint64) func() (batchStart, batchEnd uint64, ok bool) {
	current := startID

	return func() (uint64, uint64, bool) {
		if current > endID {
			return 0, 0, false
		}
		start := current
		end := current + batchSize
		current = end
		return start, end, true
	}
}

func processBatch(ctx context.Context, fromDB Database, toDB Database, b batch) error {
	rows, err := fromDB.LoadRows(ctx, b.start, b.end)
	if err != nil {
		return fmt.Errorf("error loading rows in range [%d, %d): %w", b.start, b.end, err)
	}

	if len(rows) == 0 {
		return nil
	}

	if err := toDB.SaveRows(ctx, rows); err != nil {
		return fmt.Errorf("error saving rows in range [%d, %d): %w", b.start, b.end, err)
	}

	return nil
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())

	transientSubstrings := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"i/o timeout",
		"EOF",
	}

	for _, sub := range transientSubstrings {
		if strings.Contains(errStr, sub) {
			return true
		}
	}
	return false
}

func worker(
	ctx context.Context,
	id int,
	wg *sync.WaitGroup,
	fromDB Database,
	toDB Database,
	jobs <-chan batch,
	errs chan<- error,
) {
	defer wg.Done()

	for {
		select {
		case j, ok := <-jobs:
			if !ok {
				return
			}

			log.Printf("[Worker %d] Processing batch: [%d, %d)", id, j.start, j.end)

			var currentErr error
			for i := range maxRetries {
				if ctx.Err() != nil {
					return
				}

				currentErr = processBatch(ctx, fromDB, toDB, j)
				if currentErr == nil {
					break
				}
				if !isTransient(currentErr) {
					log.Printf(
						"[Worker %d] Unrecoverable error on batch [%d, %d): %v. Aborting.",
						id,
						j.start,
						j.end,
						currentErr,
					)
					errs <- currentErr
					return
				}

				log.Printf(
					"[Worker %d] Attempt %d/%d failed for batch [%d, %d): %v. Retrying in %v...",
					id,
					i+1,
					maxRetries,
					j.start,
					j.end,
					currentErr,
					retryDelay,
				)

				select {
				case <-time.After(retryDelay):
				case <-ctx.Done():
					return
				}
			}

			if currentErr != nil {
				log.Printf(
					"[Worker %d] Failed to process batch [%d, %d) after %d attempts.",
					id,
					j.start,
					j.end,
					maxRetries,
				)
				errs <- currentErr
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func copyTableLogic(ctx context.Context, fromDB Database, toDB Database, full bool) error {
	endID, err := fromDB.GetMaxID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get max ID from source: %w", err)
	}

	var startID uint64
	if full {
		startID = 0
	} else {
		startID, err = toDB.GetMaxID(ctx)
		if err != nil {
			return fmt.Errorf("failed to get max ID from destination: %w", err)
		}
		if startID > 0 {
			startID++
		}
	}

	if startID >= endID {
		return nil
	}

	jobs := make(chan batch, numWorkers)
	errs := make(chan error, 1)

	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)
		go worker(ctx, i, &wg, fromDB, toDB, jobs, errs)
	}

	go func() {
		defer close(jobs)

		nextBatcher := batcher(startID, endID, batchSize)
		for {
			batchStart, batchEnd, ok := nextBatcher()
			if !ok {
				break
			}

			select {
			case jobs <- batch{start: batchStart, end: batchEnd}:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errs)
	}()

	if err := <-errs; err != nil {
		return err
	}

	return nil
}

func CopyTable(fromName string, toName string, full bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	fromDB, err := Connect(ctx, fromName)
	if err != nil {
		return fmt.Errorf("failed to connect to source DB %s: %w", fromName, err)
	}
	defer fromDB.Close()

	toDB, err := Connect(ctx, toName)
	if err != nil {
		return fmt.Errorf("failed to connect to destination DB %s: %w", toName, err)
	}
	defer toDB.Close()

	if err := copyTableLogic(ctx, fromDB, toDB, full); err != nil {
		cancel()
		return fmt.Errorf("copy process failed: %w", err)
	}

	return nil
}

func main() {
	const prodDSN = "postgres://postgres:secret@localhost:5433/prod_db"
	const statsDSN = "postgres://postgres:secret@localhost:5434/stats_db"

	log.Printf("Source: %s", prodDSN)
	log.Printf("Destination: %s", statsDSN)

	fullCopy := false

	if err := CopyTable(prodDSN, statsDSN, fullCopy); err != nil {
		log.Fatalf("Copy failed: %v", err)
	}
}
