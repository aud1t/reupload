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

func TestCopyTableLogic(t *testing.T) {
	testCases := []struct {
		name       string
		sourceData []Row
		destData   []Row
		fullFlag   bool
		wantData   []Row
		wantErr    bool
	}{
		{
			name: "Full copy on empty destination",
			sourceData: []Row{
				{uint64(1), []byte(`{"a": 1}`)},
				{uint64(100), []byte(`{"c": 3}`)},
			},
			destData: []Row{},
			fullFlag: true,
			wantData: []Row{
				{uint64(1), []byte(`{"a": 1}`)},
				{uint64(100), []byte(`{"c": 3}`)},
			},
			wantErr: false,
		},
		{
			name: "Incremental copy",
			sourceData: []Row{
				{uint64(1), []byte(`{}`)},
				{uint64(2), []byte(`{}`)},
				{uint64(3), []byte(`{}`)},
			},
			destData: []Row{
				{uint64(1), []byte(`{}`)},
			},
			fullFlag: false,
			wantData: []Row{
				{uint64(1), []byte(`{}`)},
				{uint64(2), []byte(`{}`)},
				{uint64(3), []byte(`{}`)},
			},
			wantErr: false,
		},
		{
			name: "Destination is already up to date",
			sourceData: []Row{
				{uint64(1), []byte(`{}`)},
				{uint64(2), []byte(`{}`)},
			},
			destData: []Row{
				{uint64(1), []byte(`{}`)},
				{uint64(2), []byte(`{}`)},
			},
			fullFlag: false,
			wantData: []Row{
				{uint64(1), []byte(`{}`)},
				{uint64(2), []byte(`{}`)},
			},
			wantErr: false,
		},
		{
			name:       "Source is empty",
			sourceData: []Row{},
			destData: []Row{
				{uint64(1), []byte(`{}`)},
			},
			fullFlag: false,
			wantData: []Row{
				{uint64(1), []byte(`{}`)},
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDB := NewMockDB()
			destDB := NewMockDB()
			sourceDB.SaveRows(context.Background(), tc.sourceData)
			destDB.SaveRows(context.Background(), tc.destData)

			err := copyTableLogic(context.Background(), sourceDB, destDB, tc.fullFlag)

			if (err != nil) != tc.wantErr {
				t.Fatalf("copyTableLogic() error = %v, wantErr %v", err, tc.wantErr)
			}

			wantMap := make(map[uint64]Row)
			for _, row := range tc.wantData {
				wantMap[row[0].(uint64)] = row
			}

			if !reflect.DeepEqual(destDB.data, wantMap) {
				t.Errorf("Destination DB data mismatch.\nGot:  %v\nWant: %v", destDB.data, wantMap)
			}
		})
	}
}
