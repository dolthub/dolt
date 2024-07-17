// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsess

import (
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// SessionCache caches various pieces of expensive to compute information to speed up future lookups in the session.
type SessionCache struct {
	// indexes is keyed by table schema hash
	indexes map[doltdb.DataCacheKey]map[string][]sql.Index
	// tables is keyed by table root value
	tables map[doltdb.DataCacheKey]map[TableCacheKey]sql.Table
	// tableMaps is keyed by a digest of the table list of table names
	tableMaps map[uint64]map[string]string

	// TODO: cache views/triggers by schema fragment hash
	views    map[doltdb.DataCacheKey]map[TableCacheKey]sql.ViewDefinition
	triggers map[TableSchemaKey][]sql.TriggerDefinition

	// writers are keyed by table schema hash
	writers map[doltdb.DataCacheKey]*WriterState
	// checks is keyed by table schema hash
	checks map[doltdb.DataCacheKey][]sql.CheckDefinition

	mu sync.RWMutex
}

// DatabaseCache stores databases and their initial states, offloading the compute / IO involved in resolving a
// database name to a particular database. This is safe only because the database objects themselves don't have any
// handles to data or state, but always defer to the session. Keys in the secondary map are revision specifier strings
type DatabaseCache struct {
	// revisionDbs caches databases by name. The name is always lower case and revision qualified
	revisionDbs map[revisionDbCacheKey]SqlDatabase
	// initialDbStates caches the initial state of databases by name for a given noms root, which is the primary key.
	// The secondary key is the lower-case revision-qualified database name.
	initialDbStates map[doltdb.DataCacheKey]map[string]InitialDbState
	// sessionVars records a key for the most recently used session vars for each database in the session
	sessionVars map[string]sessionVarCacheKey

	mu sync.RWMutex
}

type revisionDbCacheKey struct {
	dbName        string
	requestedName string
}

type sessionVarCacheKey struct {
	root doltdb.DataCacheKey
	head string
}

const maxCachedKeys = 64

func newSessionCache() *SessionCache {
	return &SessionCache{}
}

func newDatabaseCache() *DatabaseCache {
	return &DatabaseCache{
		sessionVars: make(map[string]sessionVarCacheKey),
	}
}

// CacheTableIndexes caches all indexes for the table with the name given
func (c *SessionCache) CacheTableIndexes(key doltdb.DataCacheKey, table string, indexes []sql.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()

	table = strings.ToLower(table)

	if c.indexes == nil {
		c.indexes = make(map[doltdb.DataCacheKey]map[string][]sql.Index)
	}
	if len(c.indexes) > maxCachedKeys {
		for k := range c.indexes {
			delete(c.indexes, k)
		}
	}

	tableIndexes, ok := c.indexes[key]
	if !ok {
		tableIndexes = make(map[string][]sql.Index)
		c.indexes[key] = tableIndexes
	}

	tableIndexes[table] = indexes
}

// GetTableIndexesCache returns the cached index information for the table named, and whether the cache was present
func (c *SessionCache) GetTableIndexesCache(key doltdb.DataCacheKey, table string) ([]sql.Index, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.indexes == nil {
		return nil, false
	}

	tableIndexes, ok := c.indexes[key]
	if !ok {
		return nil, false
	}
	table = strings.ToLower(table)

	indexes, ok := tableIndexes[table]
	return indexes, ok
}

// CacheTable caches a sql.Table implementation for the table named
func (c *SessionCache) CacheTable(key doltdb.DataCacheKey, tableName TableCacheKey, table sql.Table) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables == nil {
		c.tables = make(map[doltdb.DataCacheKey]map[TableCacheKey]sql.Table)
	}
	if len(c.tables) > maxCachedKeys {
		for k := range c.tables {
			delete(c.tables, k)
		}
	}

	tablesForKey, ok := c.tables[key]
	if !ok {
		tablesForKey = make(map[TableCacheKey]sql.Table)
		c.tables[key] = tablesForKey
	}

	tablesForKey[tableName.ToLower()] = table
}

// ClearTableCache removes all cache info for all tables at all cache keys
func (c *SessionCache) ClearTableCache() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.tables {
		delete(c.tables, k)
	}
}

type TableCacheKey struct {
	Name   string
	Schema string
}

func (k TableCacheKey) ToLower() TableCacheKey {
	return TableCacheKey{
		Name:   strings.ToLower(k.Name),
		Schema: strings.ToLower(k.Schema),
	}
}

// GetCachedTable returns the cached sql.Table for the table named, and whether the cache was present
func (c *SessionCache) GetCachedTable(key doltdb.DataCacheKey, tableName TableCacheKey) (sql.Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.tables == nil {
		return nil, false
	}

	tablesForKey, ok := c.tables[key]
	if !ok {
		return nil, false
	}

	table, ok := tablesForKey[tableName.ToLower()]
	return table, ok
}

// GetCachedWriterState returns the cached WriterState for the table named, and whether the cache was present
func (c *SessionCache) GetCachedWriterState(key doltdb.DataCacheKey) (*WriterState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schemaState, ok := c.writers[key]
	return schemaState, ok
}

// CacheWriterState caches a WriterState implementation for the table named
func (c *SessionCache) CacheWriterState(key doltdb.DataCacheKey, state *WriterState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.writers == nil {
		c.writers = make(map[doltdb.DataCacheKey]*WriterState)
	}
	if len(c.writers) > maxCachedKeys {
		for k := range c.writers {
			delete(c.writers, k)
		}
	}

	c.writers[key] = state
}

func (c *SessionCache) GetCachedTableChecks(key doltdb.DataCacheKey) ([]sql.CheckDefinition, bool) {
	checks, ok := c.checks[key]
	return checks, ok
}

// CacheTableChecks caches sql.CheckConstraints for the table named
func (c *SessionCache) CacheTableChecks(key doltdb.DataCacheKey, checks []sql.CheckDefinition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.checks == nil {
		c.checks = make(map[doltdb.DataCacheKey][]sql.CheckDefinition)
	}
	if len(c.checks) > maxCachedKeys {
		for k := range c.checks {
			delete(c.checks, k)
		}
	}

	c.checks[key] = checks
}

func (c *SessionCache) GetCachedTableMap(key uint64) (map[string]string, bool) {
	tables, ok := c.tableMaps[key]
	return tables, ok
}

// CacheTableChecks caches sql.CheckConstraints for the table named
func (c *SessionCache) CacheTableMap(key uint64, tables map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tableMaps == nil {
		c.tableMaps = make(map[uint64]map[string]string)
	}
	if len(c.tableMaps) > maxCachedKeys {
		for k := range c.tableMaps {
			delete(c.tableMaps, k)
		}
	}

	c.tableMaps[key] = tables
}

// CacheViews caches all views in a database for the cache key given
func (c *SessionCache) CacheViews(key doltdb.DataCacheKey, views []sql.ViewDefinition, schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.views == nil {
		c.views = make(map[doltdb.DataCacheKey]map[TableCacheKey]sql.ViewDefinition)
	}
	if len(c.views) > maxCachedKeys {
		for k := range c.views {
			delete(c.views, k)
		}
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		viewsForKey = make(map[TableCacheKey]sql.ViewDefinition)
		c.views[key] = viewsForKey
	}

	for i := range views {
		viewName := TableCacheKey{
			Name:   strings.ToLower(views[i].Name),
			Schema: strings.ToLower(schema),
		}
		viewsForKey[viewName] = views[i]
	}
}

// ViewsCached returns whether this cache has been initialized with the set of views yet
func (c *SessionCache) ViewsCached(key doltdb.DataCacheKey) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.views == nil {
		return false
	}

	_, ok := c.views[key]
	return ok
}

// GetCachedViewDefinition returns the cached view named, and whether the cache was present
func (c *SessionCache) GetCachedViewDefinition(key doltdb.DataCacheKey, viewName TableCacheKey) (sql.ViewDefinition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.views == nil {
		return sql.ViewDefinition{}, false
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		return sql.ViewDefinition{}, false
	}

	table, ok := viewsForKey[viewName.ToLower()]
	return table, ok
}

type TableSchemaKey struct {
	key    doltdb.DataCacheKey
	schema string
}

// CacheTriggers caches all views in a database for the cache key given
func (c *SessionCache) CacheTriggers(key doltdb.DataCacheKey, triggers []sql.TriggerDefinition, schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.triggers == nil {
		c.triggers = make(map[TableSchemaKey][]sql.TriggerDefinition)
	}
	if len(c.triggers) > maxCachedKeys {
		for k := range c.triggers {
			delete(c.triggers, k)
		}
	}

	schKey := TableSchemaKey{key: key, schema: schema}
	_, ok := c.triggers[schKey]
	if !ok {
		// create backing array to differentiate no triggers/no cache
		c.triggers[schKey] = make([]sql.TriggerDefinition, 0)
	}

	c.triggers[schKey] = append(c.triggers[schKey], triggers...)
}

// GetCachedTriggers returns the cached view named, and whether the cache was present
func (c *SessionCache) GetCachedTriggers(key doltdb.DataCacheKey, schema string) ([]sql.TriggerDefinition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schKey := TableSchemaKey{key: key, schema: schema}

	triggers, ok := c.triggers[schKey]
	return triggers, ok
}

// GetCachedRevisionDb returns the cached revision database named, and whether the cache was present
func (c *DatabaseCache) GetCachedRevisionDb(revisionDbName string, requestedName string) (SqlDatabase, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.revisionDbs == nil {
		return nil, false
	}

	db, ok := c.revisionDbs[revisionDbCacheKey{
		dbName:        revisionDbName,
		requestedName: requestedName,
	}]
	return db, ok
}

// CacheRevisionDb caches the revision database named
func (c *DatabaseCache) CacheRevisionDb(database SqlDatabase) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.revisionDbs == nil {
		c.revisionDbs = make(map[revisionDbCacheKey]SqlDatabase)
	}

	if len(c.revisionDbs) > maxCachedKeys {
		for k := range c.revisionDbs {
			delete(c.revisionDbs, k)
		}
	}

	c.revisionDbs[revisionDbCacheKey{
		dbName:        strings.ToLower(database.RevisionQualifiedName()),
		requestedName: database.RequestedName(),
	}] = database
}

// GetCachedInitialDbState returns the cached initial state for the revision database named, and whether the cache
// was present
func (c *DatabaseCache) GetCachedInitialDbState(key doltdb.DataCacheKey, revisionDbName string) (InitialDbState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.initialDbStates == nil {
		return InitialDbState{}, false
	}

	dbsForKey, ok := c.initialDbStates[key]
	if !ok {
		return InitialDbState{}, false
	}

	db, ok := dbsForKey[revisionDbName]
	return db, ok
}

// CacheInitialDbState caches the initials state for the revision database named
func (c *DatabaseCache) CacheInitialDbState(key doltdb.DataCacheKey, revisionDbName string, state InitialDbState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialDbStates == nil {
		c.initialDbStates = make(map[doltdb.DataCacheKey]map[string]InitialDbState)
	}

	if len(c.initialDbStates) > maxCachedKeys {
		for k := range c.initialDbStates {
			delete(c.initialDbStates, k)
		}
	}

	dbsForKey, ok := c.initialDbStates[key]
	if !ok {
		dbsForKey = make(map[string]InitialDbState)
		c.initialDbStates[key] = dbsForKey
	}

	dbsForKey[revisionDbName] = state
}

// CacheSessionVars updates the session var cache for the given branch state and transaction and returns whether it
// was updated. If it was updated, session vars need to be set for the state and transaction given. Otherwise they
// haven't changed and can be reused.
func (c *DatabaseCache) CacheSessionVars(branchState *branchState, transaction *DoltTransaction) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	dbBaseName := branchState.dbState.dbName

	existingKey, found := c.sessionVars[dbBaseName]
	root, hasRoot := transaction.GetInitialRoot(dbBaseName)
	if !hasRoot {
		return true
	}

	newKey := sessionVarCacheKey{
		root: doltdb.DataCacheKey{Hash: root},
		head: strings.ToLower(branchState.head),
	}

	c.sessionVars[dbBaseName] = newKey
	return !found || existingKey != newKey
}

func (c *DatabaseCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionVars = make(map[string]sessionVarCacheKey)
	c.revisionDbs = make(map[revisionDbCacheKey]SqlDatabase)
	c.initialDbStates = make(map[doltdb.DataCacheKey]map[string]InitialDbState)
}
