package autoincr

import (
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	Request(tableName string, val interface{}) (bool, error)
	Confirm(tableName string, val interface{})
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

// Request is used by an integrator to request whether the wanted auto increment key is valid for use. Requests do not
// necessarily update the tracker as an integrator may query the same auto increment value multiple times (in the
// information schema for example). A request updates the tracker when it is greater than the current value stored for
// the table.
func (a *autoIncrementTracker) Request(tableName string, val interface{}) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	newVal, err := ConvertIntTypeToUint(val)
	if err != nil {
		return false, err
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

	a.tables[tableName] = val
	a.written[tableName] = false
	return true, nil
}

// Confirm is used when an auto increment has actually been written to a table.
func (a *autoIncrementTracker) Confirm(tableName string, key interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.tables[tableName] = key
	a.written[tableName] = true
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
		return 0, fmt.Errorf("error: auto increment is not a numeric type")
	}
}
