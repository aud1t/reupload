package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

type Row []interface{}

type Database interface {
	io.Closer
	GetMaxID(ctx context.Context) (uint64, error)
	GetMinID(ctx context.Context) (uint64, error)
	LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)
	SaveRows(ctx context.Context, rows []Row) error
}

type DBConnector interface {
	Connect(ctx context.Context, dbname string) (Database, error)
}

type PostgresConnector struct {
}

func (pc *PostgresConnector) Connect(ctx context.Context, dbname string) (Database, error) {
	return nil, fmt.Errorf("PostgresConnector.Connect function is not implemented for db: %s", dbname)
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

func newBatchGenerator(startID, endID, batchSize uint64) func() (batchStart, batchEnd uint64, ok bool) {
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

const (
	numWorkers  = 10
	maxRetries  = 3
	retryDelay  = 2 * time.Second
	copyTimeout = 24 * time.Hour
	batchSize   = 10_000
)

type batch struct {
	start uint64
	end   uint64
}

func processBatch(ctx context.Context, taskID int, fromDB Database, toDB Database, b batch) error {
	log.Printf("[Task %d] Processing batch: [%d, %d)", taskID, b.start, b.end)
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
	log.Printf("[Task %d] Successfully saved %d rows from batch [%d, %d).", taskID, len(rows), b.start, b.end)
	return nil
}

func runParallelCopy(ctx context.Context, fromDB, toDB Database, startID, endID uint64) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)

	batchGen := newBatchGenerator(startID, endID, batchSize)
	taskID := 0
	for {
		start, end, ok := batchGen()
		if !ok {
			break
		}

		s, e, id := start, end, taskID
		taskID++

		g.Go(func() error {
			b := batch{start: s, end: e}
			var currentErr error
			for i := range maxRetries {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				currentErr = processBatch(gCtx, id, fromDB, toDB, b)
				if currentErr == nil {
					return nil
				}
				if !isTransient(currentErr) {
					log.Printf("[Task %d] Unrecoverable error: %v. Aborting.", id, currentErr)
					return currentErr
				}

				log.Printf("[Task %d] Attempt %d/%d failed: %v. Retrying...", id, i+1, maxRetries, currentErr)
				select {
				case <-time.After(retryDelay):
				case <-gCtx.Done():
					return gCtx.Err()
				}
			}
			log.Printf("[Task %d] Failed after %d retries: %v", id, maxRetries, currentErr)
			return currentErr
		})
	}

	return g.Wait()
}

func CopyTable(
	ctx context.Context,
	connector DBConnector, 
	fromName string, 
	toName string, 
	full bool,
) error {
	opCtx, opCancel := context.WithTimeout(ctx, copyTimeout)
	defer opCancel()

	fromDB, err := connector.Connect(opCtx, fromName)
	if err != nil {
		return fmt.Errorf("failed to connect to source DB %s: %w", fromName, err)
	}
	defer fromDB.Close()

	toDB, err := connector.Connect(opCtx, toName)
	if err != nil {
		return fmt.Errorf("failed to connect to destination DB %s: %w", toName, err)
	}
	defer toDB.Close()

	endID, err := fromDB.GetMaxID(opCtx)
	if err != nil {
		return fmt.Errorf("failed to get max ID from source: %w", err)
	}

	var startID uint64
	if full {
		startID, err = fromDB.GetMinID(opCtx)
		if err != nil {
			return fmt.Errorf("failed to get min ID from source: %w", err)
		}
	} else {
		startID, err = toDB.GetMaxID(opCtx)
		if err != nil {
			return fmt.Errorf("failed to get max ID from destination: %w", err)
		}
		if startID > 0 {
			startID++
		}
	}

	if startID >= endID {
		log.Println("Destination is up to date.")
		return nil
	}

	if err := runParallelCopy(opCtx, fromDB, toDB, startID, endID); err != nil {
		return fmt.Errorf("copy process failed: %w", err)
	}

	return nil
}

func main() {
	const prodDSN = "postgres://postgres:secret@localhost:5433/prod_db"
	const statsDSN = "postgres://postgres:secret@localhost:5434/stats_db"

	log.Printf("Source: %s", prodDSN)
	log.Printf("Destination: %s", statsDSN)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fullCopy := false

	pgConnector := &PostgresConnector{}
	err := CopyTable(ctx, pgConnector, prodDSN, statsDSN, fullCopy)
	if err != nil {
		// graceful shutdown
		if err == context.Canceled || err == context.DeadlineExceeded {
			log.Printf("Shutdown signal received. Exiting gracefully.")
		} else {
			log.Fatalf("FATAL ERROR: %v", err)
		}
	}
}
