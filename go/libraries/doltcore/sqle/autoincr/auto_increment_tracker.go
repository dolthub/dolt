package autoincr

import (
	"errors"
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	Next(tableName string) (interface{}, error)
	InitTable(tableName string, val interface{})
	Greater(val1 interface{}, val2 interface{}) bool
	Current(tableName string) interface{}
}

var ErrTableNotInitialized = errors.New("Table not initializaed")

// AutoIncrementTracker is a global map that tracks the next auto increment value for each table in each database.
// It is primarily used for concurrent transactions on an auto incremented table.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		tables:  make(map[string]interface{}),
		written: make(map[string]bool),
	}
}

type autoIncrementTracker struct {
	tables  map[string]interface{}
	written map[string]bool
	mu      sync.Mutex
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

func (a *autoIncrementTracker) Next(tableName string) (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	it, ok := a.tables[tableName]
	if !ok {
		return nil, ErrTableNotInitialized
	}

	val, err := ConvertIntTypeToUint(it)
	if err != nil {
		return nil, err
	}

	a.tables[tableName] = val + 1

	return val, nil
}

func (a *autoIncrementTracker) InitTable(tableName string, val interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.tables[tableName] = val
}

func (a *autoIncrementTracker) Greater(val1 interface{}, val2 interface{}) bool {
	v1, _ := ConvertIntTypeToUint(val1)
	v2, _ := ConvertIntTypeToUint(val2)

	return v1 > v2
}

func (a *autoIncrementTracker) Current(tableName string) interface{} {
	return a.tables[tableName]
}

func ConvertIntTypeToUint(val interface{}) (uint64, error) {
	switch t := val.(type) {
	case int:
		return uint64(t), nil
	case int8:
		return uint64(t), nil
	case int16:
		return uint64(t), nil
	case int32:
		return uint64(t), nil
	case int64:
		return uint64(t), nil
	case uint:
		return uint64(t), nil
	case uint8:
		return uint64(t), nil
	case uint16:
		return uint64(t), nil
	case uint32:
		return uint64(t), nil
	case uint64:
		return t, nil
	case float32:
		return uint64(t), nil
	case float64:
		return uint64(t), nil
	case interface{}:
		return 0, nil
	default:
		return 0, fmt.Errorf("error: auto increment is not a numeric type")
	}
}
