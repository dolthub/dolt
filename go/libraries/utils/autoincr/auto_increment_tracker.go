package autoincr

import (
	"fmt"
)

type AutoIncrementTracker interface {
	Reserve(tableName string, val interface{}, force bool) (bool, error)
}

// AutoIncrementTracker is a global map that tracks the next auto increment value for each table in each database.
// It is primarily used for concurrent transactions on an auto incremented table.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		tables: make(map[string]uint64),
		written: make(map[string]bool),
	}
}

type autoIncrementTracker struct {
	tables map[string]uint64
	written map[string]bool
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

// Reserve tells an integrator whether the wanted auto increment key toReserved has already been used by another transaction.
// A key has already been used if the tracker has a larger or equal to value.
func (a *autoIncrementTracker) Reserve(tableName string, toReserveKey interface{}, force bool) (bool, error) {
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

	if newVal <= currVal {
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
	default:
		return 0, fmt.Errorf("error: auto increment is not int type")
	}
}
