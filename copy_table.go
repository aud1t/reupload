package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"
)

type Row []interface{}

type Database interface {
	io.Closer
	GetMaxID(ctx context.Context) (uint64, error)
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

const (
	batchSize   uint64 = 10_000
	copyTimeout        = 24 * time.Hour
)

func newBatchGenerator(startID, endID, batchSize uint64) func() (start, end uint64, ok bool) {
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

func CopyTable(connector DBConnector, fromName string, toName string, full bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), copyTimeout)
	defer cancel()

	fromDB, err := connector.Connect(ctx, fromName)
	if err != nil {
		return fmt.Errorf("failed to connect to source DB %s: %w", fromName, err)
	}
	defer fromDB.Close()

	toDB, err := connector.Connect(ctx, toName)
	if err != nil {
		return fmt.Errorf("failed to connect to destination DB %s: %w", toName, err)
	}
	defer toDB.Close()

	endID, err := fromDB.GetMaxID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get max ID from source: %w", err)
	}

	var startID uint64
	if !full {
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

	batchGen := newBatchGenerator(startID, endID, batchSize)
	for {
		batchStart, batchEnd, ok := batchGen()
		if !ok {
			break
		}

		rows, err := fromDB.LoadRows(ctx, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("error loading rows in range [%d, %d): %w", batchStart, batchEnd, err)
		}

		if len(rows) == 0 {
			continue
		}

		if err := toDB.SaveRows(ctx, rows); err != nil {
			return fmt.Errorf("error saving rows in range [%d, %d): %w", batchStart, batchEnd, err)
		}
	}

	return nil
}

func main() {
	const prodDSN = "postgres://postgres:secret@localhost:5433/prod_db"
	const statsDSN = "postgres://postgres:secret@localhost:5434/stats_db"

	log.Printf("Source: %s", prodDSN)
	log.Printf("Destination: %s", statsDSN)

	fullCopy := false

	pgConnector := &PostgresConnector{}
	err := CopyTable(pgConnector, prodDSN, statsDSN, fullCopy)
	if err != nil {
		log.Fatalf("Copy failed: %v", err)
	}
}
