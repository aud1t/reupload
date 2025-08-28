package main

import (
	"context"
	"database/sql"
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

// Это функция где-то реализована
func Connect(ctx context.Context, dsn string) (Database, error) {
	return nil, nil
}

// CopyTable копирует данные из одной таблицы в другую.
// Если full=false - то продолжить переливку данных с места прошлой ошибки
// Если full=true - то перелить все данные
func CopyTable(fromName string, toName string, full bool) error {
	const batchSize uint64 = 10000

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	fromDB, err := Connect(ctx, fromDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to source DB %s: %w", fromDSN, err)
	}
	defer fromDB.Close()

	toDB, err := Connect(ctx, toDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to destination DB %s: %w", toDSN, err)
	}
	defer toDB.Close()

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

	for currentID := startID; currentID <= endID; {
		batchEndID := currentID + batchSize
		
		rows, err := fromDB.LoadRows(ctx, currentID, batchEndID)
		if err != nil {
			return fmt.Errorf("error loading rows in range [%d, %d): %w", currentID, batchEndID, err)
		}

		if len(rows) > 0 {
			if err := toDB.SaveRows(ctx, rows); err != nil {
				return fmt.Errorf("error saving rows in range [%d, %d): %w", currentID, batchEndID, err)
			}
		}
		
		currentID = batchEndID

		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
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

	if err := CopyTable(prodDSN, statsDSN, fullCopy); err != nil {
		log.Fatalf("Copy failed: %v", err)
	}
}