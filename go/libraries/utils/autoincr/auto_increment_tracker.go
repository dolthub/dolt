package autoincr

import "fmt"

type AutoIncrementTracker interface {
	Reserve(dbname, tablename string, val interface{}) (bool, error)
}

type AutoIncrementTrackerSubscriber interface {
	ReserveAutoIncrementValueForTable(tableName string, val interface{}) (bool, error)
}

// AutoIncrementTracker is a global map that tracks the next auto increment value for each table in each database.
// It is primarily used for concurrent transactions on an auto incremented table.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		tables: make(map[string]map[string]uint64),
	}
}

// AutoIncrementTrackerSubscriber subscribes to an AutoIncrementTracker by wrapping a pointer and a database name.
// Useful for deeper in the table engine where root values and databases should not be conflated.
func NewAutoIncrementTableSubscriber(dbName string, ai AutoIncrementTracker) AutoIncrementTrackerSubscriber {
	return &autoIncrementTableSubscriber{
		dbName: dbName,
		ai: ai,
	}
}

type autoIncrementTracker struct {
	tables map[string]map[string]uint64
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

func (a *autoIncrementTracker) Get(dbname, tableName string) (uint64, bool) {
	return a.tables[dbname][tableName], true
}

func (a *autoIncrementTracker) Set(dbname, tableName string, val uint64) bool {
	dbTable := a.tables[dbname]
	if dbTable == nil {
		a.tables[dbname] = make(map[string]uint64)
		dbTable = a.tables[dbname]
	}

	dbTable[tableName] = val

	return true
}

func (a *autoIncrementTracker) Reserve(dbname, tableName string, val interface{}) (bool, error) {
	dbTable := a.tables[dbname]
	if dbTable == nil {
		a.tables[dbname] = make(map[string]uint64)
		dbTable = a.tables[dbname]
	}

	currVal := dbTable[tableName]
	newVal, err := convertIntToUint(val)
	if err != nil {
		return false, err
	}

	if newVal <= currVal {
		return false, nil
	}

	dbTable[tableName] = newVal
	return true, nil
}

type autoIncrementTableSubscriber struct {
	dbName string
	ai AutoIncrementTracker
}

var _ AutoIncrementTrackerSubscriber = (*autoIncrementTableSubscriber)(nil)

func (a *autoIncrementTableSubscriber) ReserveAutoIncrementValueForTable(tableName string, val interface{}) (bool, error) {
	if a.ai == nil {
		return false, nil
	}

	return a.ai.Reserve(a.dbName, tableName, val)
}

func convertIntToUint(val interface{}) (uint64, error) {
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
