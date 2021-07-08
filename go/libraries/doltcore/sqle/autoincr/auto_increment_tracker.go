package autoincr

import (
	"errors"
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	Mark(tableName string, val interface{}) error
	// NextOrInit returns the next auto increment value to be used by a table. If a table is not initialized in the counter
	// it will used the value stored in disk.
	NextOrInit(tableName string, val interface{}) (interface{}, error)

	Reset(tableName string, val interface{})
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

func (a *autoIncrementTracker) NextOrInit(tableName string, expected interface{}) (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	it, ok := a.valuePerTable[tableName]
	if !ok {
		it = expected
	}

	val, err := ConvertIntTypeToUint(it)
	if err != nil {
		return val, err
	}

	a.valuePerTable[tableName] = val + 1

	return val, nil
}

func (a *autoIncrementTracker) Mark(tableName string, val interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentVal := a.valuePerTable[tableName]
	ok, err := geq(val, currentVal)
	if err != nil {
		return err
	}

	if ok {
		toInt, err := ConvertIntTypeToUint(val)
		if err != nil {
			return err
		}

		a.valuePerTable[tableName] = toInt + 1
	}

	return nil
}

func (a *autoIncrementTracker) Reset(tableName string, val interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.valuePerTable[tableName] = val
}

func geq(val1 interface{}, val2 interface{}) (bool, error) {
	v1, err := ConvertIntTypeToUint(val1)
	if err != nil {
		return false, err
	}

	v2, err := ConvertIntTypeToUint(val2)
	if err != nil {
		return false, err
	}

	return v1 >= v2, nil
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
