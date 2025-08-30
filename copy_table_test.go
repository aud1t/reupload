package main

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

type MockDB struct {
	mu    sync.RWMutex
	data  map[uint64]Row
	maxID atomic.Uint64
}

func NewMockDB() *MockDB {
	return &MockDB{
		data: make(map[uint64]Row),
	}
}

func (m *MockDB) GetMaxID(ctx context.Context) (uint64, error) {
	return m.maxID.Load(), nil
}

func (m *MockDB) LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var rows []Row
	for id, row := range m.data {
		if id >= minID && id < maxID {
			rows = append(rows, row)
		}
	}

	// Сортируем для предсказуемости в тестах
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(uint64) < rows[j][0].(uint64)
	})
	return rows, nil
}

func (m *MockDB) SaveRows(ctx context.Context, rows []Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, row := range rows {
		id := row[0].(uint64)
		m.data[id] = row

		if currMaxID := m.maxID.Load(); id > currMaxID {
			m.maxID.Store(id)
		}
	}

	return nil
}

func (m *MockDB) Close() error {
	return nil
}

func TestCopyTableLogic_FullCopy(t *testing.T) {
	sourceDB := NewMockDB()
	destDB := NewMockDB()

	testData := []Row{
		{uint64(1), []byte(`{"a": 1}`)},
		{uint64(2), []byte(`{"b": 2}`)},
		{uint64(100), []byte(`{"c": 3}`)},
	}
	sourceDB.SaveRows(context.Background(), testData)

	err := copyTableLogic(context.Background(), sourceDB, destDB, true)

	if err != nil {
		t.Fatalf("copyTableLogic failed with error: %v", err)
	}

	if !reflect.DeepEqual(sourceDB.data, destDB.data) {
		t.Errorf("Destination DB data does not match source DB data.\nGot: %v\nWant: %v", destDB.data, sourceDB.data)
	}
}

func TestCopyTableLogic_IncrementalCopy(t *testing.T) {
	sourceDB := NewMockDB()
	destDB := NewMockDB()

	sourceDB.SaveRows(context.Background(), []Row{
		{uint64(1), []byte(`{"a": 1}`)},
		{uint64(2), []byte(`{"b": 2}`)},
		{uint64(3), []byte(`{"c": 3}`)},
		{uint64(4), []byte(`{"c": 4}`)},
		{uint64(5), []byte(`{"c": 5}`)},
		{uint64(6), []byte(`{"c": 6}`)},
	})

	destDB.SaveRows(context.Background(), []Row{
		{uint64(1), []byte(`{"a": 1}`)},
		{uint64(2), []byte(`{"b": 2}`)},
	})

	err := copyTableLogic(context.Background(), sourceDB, destDB, false)

	if err != nil {
		t.Fatalf("copyTableLogic failed with error: %v", err)
	}

	if !reflect.DeepEqual(sourceDB.data, destDB.data) {
		t.Errorf("Destination DB data does not match source DB data.\nGot: %v\nWant: %v", destDB.data, sourceDB.data)
	}
}

func TestCopyTableLogic_IncrementalCopyFull(t *testing.T) {
	sourceDB := NewMockDB()
	destDB := NewMockDB()

	sourceDB.SaveRows(context.Background(), []Row{
		{uint64(1), []byte(`{"a": 1}`)},
		{uint64(2), []byte(`{"b": 2}`)},
		{uint64(3), []byte(`{"c": 3}`)},
		{uint64(4), []byte(`{"c": 4}`)},
		{uint64(5), []byte(`{"c": 5}`)},
		{uint64(6), []byte(`{"c": 6}`)},
	})

	destDB.SaveRows(context.Background(), []Row{
		{uint64(3), []byte(`{"c": 3}`)},
		{uint64(4), []byte(`{"c": 4}`)},
	})

	err := copyTableLogic(context.Background(), sourceDB, destDB, true)

	if err != nil {
		t.Fatalf("copyTableLogic failed with error: %v", err)
	}

	if !reflect.DeepEqual(sourceDB.data, destDB.data) {
		t.Errorf("Destination DB data does not match source DB data.\nGot: %v\nWant: %v", destDB.data, sourceDB.data)
	}
}

func TestCopyTableLogic_DestinationAlreadyUpToDate(t *testing.T) {
	sourceDB := NewMockDB()
	destDB := NewMockDB()

	testData := []Row{
		{uint64(1), []byte(`{}`)},
		{uint64(2), []byte(`{}`)},
	}
	sourceDB.SaveRows(context.Background(), testData)
	destDB.SaveRows(context.Background(), testData)

	err := copyTableLogic(context.Background(), sourceDB, destDB, false)

	if err != nil {
		t.Fatalf("copyTableLogic failed with error when it should have done nothing: %v", err)
	}
}