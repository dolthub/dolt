package autoincr

import (
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	Reserve(tableName string, val interface{}, force bool) (bool, error)
}

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

// Reserve tells an integrator whether the wanted auto increment key toReserved has already been used by another transaction.
// A key has already been used if the tracker has a larger or equal to value. Force is used when an autoincrement value
// is actually written to a table.
func (a *autoIncrementTracker) Reserve(tableName string, toReserveKey interface{}, force bool) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	newVal, err := ConvertIntTypeToUint(toReserveKey)
	if err != nil {
		return false, err
	}

	if force {
		a.tables[tableName] = newVal
		a.written[tableName] = true
		return true, nil
	}

	currVal := a.tables[tableName]
	written := a.written[tableName]

	if !written {
		return true, nil
	}

	currentValTyped, err := ConvertIntTypeToUint(currVal)
	if err != nil {
		return false, err
	}

	if newVal <= currentValTyped {
		return false, nil
	}

	a.tables[tableName] = newVal
	a.written[tableName] = false
	return true, nil
}

func ConvertIntTypeToUint(val interface{}) (uint64, error) {
	switch t := val.(type) {
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
	default:
		return 0, fmt.Errorf("error: auto increment is not int type")
	}
}
