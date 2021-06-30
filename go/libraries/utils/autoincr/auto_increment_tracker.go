package autoincr

import "fmt"

type AutoIncrementTracker interface {
	Reserve(tableName string, val interface{}) (bool, error)
}

// AutoIncrementTracker is a global map that tracks the next auto increment value for each table in each database.
// It is primarily used for concurrent transactions on an auto incremented table.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		tables: make(map[string]uint64),
	}
}

type autoIncrementTracker struct {
	tables map[string]uint64
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

func (a *autoIncrementTracker) Reserve(tableName string, val interface{}) (bool, error) {
	currVal := a.tables[tableName]
	newVal, err := ConvertIntTypeToUint(val)
	if err != nil {
		return false, err
	}

	if newVal <= currVal {
		return false, nil
	}

	a.tables[tableName] = newVal
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
