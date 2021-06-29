package autoincr

type AutoIncrementTracker interface {
	Get(dbname, tableName string) (uint64, bool)
	Set(dbname, tableName string, val uint64) bool
}

type AutoIncrementTrackerSubscriber interface {
	GetAutoIncrementValueForTable(tableName string) (uint64, bool)
	SetAutoIncrementValueForTable(tableName string, val uint64) bool
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

type autoIncrementTableSubscriber struct {
	dbName string
	ai AutoIncrementTracker
}

var _ AutoIncrementTrackerSubscriber = (*autoIncrementTableSubscriber)(nil)

func (a *autoIncrementTableSubscriber) GetAutoIncrementValueForTable(tableName string) (uint64, bool) {
	return a.ai.Get(a.dbName, tableName)
}

func (a *autoIncrementTableSubscriber) SetAutoIncrementValueForTable(tableName string, val uint64) bool {
	return a.ai.Set(a.dbName, tableName, val)
}
