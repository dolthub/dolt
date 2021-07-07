package autoincr

import (
	"errors"
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	// Next return the next auto increment value to be used by a table and increments its internal map
	Next(tableName string) (interface{}, error)
	// InitTable initializes the next autoincrement value to be bal
	InitTable(tableName string, val interface{})
	// Peek returns the expected next auto increment value. It is useful at insert time when an insert is larger
	// than the currently stored next auto increment key.
	Peek(tableName string) interface{}
}

var ErrTableNotInitialized = errors.New("Table not initializaed")

// AutoIncrementTracker is a global map that tracks which auto increment keys have been given for each table. At runtime
// it hands out the current key.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		valuePerTable: make(map[string]interface{}),
	}
}

type autoIncrementTracker struct {
	valuePerTable map[string]interface{}
	mu            sync.Mutex
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

func (a *autoIncrementTracker) Next(tableName string) (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	it, ok := a.valuePerTable[tableName]
	if !ok {
		return nil, ErrTableNotInitialized
	}

	val, err := ConvertIntTypeToUint(it)
	if err != nil {
		return nil, err
	}

	a.valuePerTable[tableName] = val + 1

	return val, nil
}

func (a *autoIncrementTracker) InitTable(tableName string, val interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.valuePerTable[tableName] = val
}

func (a *autoIncrementTracker) Peek(tableName string) interface{} {
	return a.valuePerTable[tableName]
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
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("error: auto increment is not a numeric type")
	}
}
