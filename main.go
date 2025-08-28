package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
)

type Row []interface{}

type Database interface {
	io.Closer
	GetMaxID(ctx context.Context) (uint64, error)
	LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error)
	SaveRows(ctx context.Context, rows []Row) error
}

type PostgresDB struct {
	db *sql.DB
	conn *pgx.Conn // соединение для использования COPY
}

func (p *PostgresDB) GetMaxID(ctx context.Context) (uint64, error) {
	var maxID sql.NullInt64
	err := p.db.QueryRowContext(ctx, "SELECT MAX(id) FROM profiles").Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("failed to query max id: %w", err)
	}
	if !maxID.Valid {
		return 0, nil // Таблица пуста
	}
	return uint64(maxID.Int64), nil
}

func (p *PostgresDB) LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) {
	query := "SELECT id, data FROM profiles WHERE id >= $1 AND id < $2"
	rows, err := p.db.QueryContext(ctx, query, minID, maxID)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows in range [%d, %d): %w", minID, maxID, err)
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		var id uint64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, Row{id, data})
	}

	return result, rows.Err()
}

func (p *PostgresDB) SaveRows(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Создаем временную таблицу, которая будет удалена после коммита.
	// Это позволяет избежать блокировок основной таблицы на время загрузки данных.
	_, err = tx.Exec(ctx, "CREATE TEMP TABLE temp_profiles (LIKE profiles INCLUDING DEFAULTS) ON COMMIT DROP")
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	sourceRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		sourceRows[i] = row
	}

	// 2. Используем COPY FROM для максимально быстрой вставки данных во временную таблицу.
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"temp_profiles"},
		[]string{"id", "data"},
		pgx.CopyFromRows(sourceRows),
	)
	if err != nil {
		return fmt.Errorf("failed to copy rows to temp table: %w", err)
	}

	// 3. Вставляем данные из временной таблицы в основную.
	// ON CONFLICT (id) DO UPDATE гарантирует идемпотентность:
	// - Если ID не существует, он будет вставлен.
	// - Если ID уже существует, поле data будет обновлено.
	_, err = tx.Exec(ctx, `
		INSERT INTO profiles (id, data)
		SELECT id, data FROM temp_profiles
		ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
	`)
	if err != nil {
		return fmt.Errorf("failed to merge from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

func (p *PostgresDB) Close() error {
	if p.conn != nil {
		p.conn.Close(context.Background())
	}
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func Connect(ctx context.Context, dsn string) (Database, error) {
	// пул соединений для обычных запросов
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}
	
	// отдельное соединение для использования команды COPY
	conn, err := stdlib.AcquireConn(db)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire pgx connection: %w", err)
	}

	log.Printf("Successfully connected to %s", dsn)
	return &PostgresDB{db: db, conn: conn}, nil
}

// CopyTable копирует данные из одной таблицы в другую.
// Если full=false - то продолжить переливку данных с места прошлой ошибки
// Если full=true - то перелить все данные
func CopyTable(fromDSN string, toDSN string, full bool) error {
	const batchSize uint64 = 80

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	log.Println("Starting table copy process...")

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
	log.Printf("Source table max ID is %d", endID)

	var startID uint64
	if full {
		startID = 0
		log.Println("Full copy mode: starting from ID 0.")
	} else {
		startID, err = toDB.GetMaxID(ctx)
		if err != nil {
			return fmt.Errorf("failed to get max ID from destination: %w", err)
		}
		if startID > 0 {
			startID++
		}
		log.Printf("Incremental copy mode: resuming from ID %d.", startID)
	}

	if startID >= endID {
		log.Println("Destination table is already up to date. Nothing to copy.")
		return nil
	}

	log.Printf("Starting copy loop from %d to %d with batch size %d", startID, endID, batchSize)
	for currentID := startID; currentID <= endID; {
		batchEndID := currentID + batchSize
		
		log.Printf("Processing batch: [%d, %d)", currentID, batchEndID)

		rows, err := fromDB.LoadRows(ctx, currentID, batchEndID)
		if err != nil {
			return fmt.Errorf("error loading rows in range [%d, %d): %w", currentID, batchEndID, err)
		}

		if len(rows) > 0 {
			if err := toDB.SaveRows(ctx, rows); err != nil {
				return fmt.Errorf("error saving rows in range [%d, %d): %w", currentID, batchEndID, err)
			}
			log.Printf("Successfully saved %d rows.", len(rows))
		} else {
			log.Println("No rows found in this range, skipping.")
		}
		
		currentID = batchEndID

		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
	}

	log.Println("Table copy process finished successfully.")
	return nil
}

func main() {
	const prodDSN = "postgres://postgres:secret@localhost:5433/prod_db"
	const statsDSN = "postgres://postgres:secret@localhost:5434/stats_db"

	log.Println("--- Running Table Copy ---")
	log.Printf("Source: %s", prodDSN)
	log.Printf("Destination: %s", statsDSN)

	fullCopy := false

	if err := CopyTable(prodDSN, statsDSN, fullCopy); err != nil {
		log.Fatalf("Copy failed: %v", err)
	}
}
