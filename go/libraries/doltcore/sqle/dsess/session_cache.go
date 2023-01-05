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
// No methods are thread safe.
type SessionCache struct {
	indexes map[doltdb.DataCacheKey]map[string][]sql.Index
	tables  map[doltdb.DataCacheKey]map[string]sql.Table
	views   map[doltdb.DataCacheKey]map[string]sql.ViewDefinition

	mu sync.RWMutex
}

func newSessionCache() *SessionCache {
	return &SessionCache{}
}

// CacheTableIndexes caches all indexes for the table with the name given
func (c *SessionCache) CacheTableIndexes(key doltdb.DataCacheKey, table string, indexes []sql.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()

	table = strings.ToLower(table)

	if c.indexes == nil {
		c.indexes = make(map[doltdb.DataCacheKey]map[string][]sql.Index)
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

	table = strings.ToLower(table)
	if c.indexes == nil {
		return nil, false
	}

	tableIndexes, ok := c.indexes[key]
	if !ok {
		return nil, false
	}

	indexes, ok := tableIndexes[table]
	return indexes, ok
}

// CacheTable caches a sql.Table implementation for the table named
func (c *SessionCache) CacheTable(key doltdb.DataCacheKey, tableName string, table sql.Table) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tableName = strings.ToLower(tableName)
	if c.tables == nil {
		c.tables = make(map[doltdb.DataCacheKey]map[string]sql.Table)
	}

	tablesForKey, ok := c.tables[key]
	if !ok {
		tablesForKey = make(map[string]sql.Table)
		c.tables[key] = tablesForKey
	}

	tablesForKey[tableName] = table
}

// ClearTableCache removes all cache info for all tables at all cache keys
func (c *SessionCache) ClearTableCache() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.tables {
		delete(c.tables, k)
	}
}

// GetCachedTable returns the cached sql.Table for the table named, and whether the cache was present
func (c *SessionCache) GetCachedTable(key doltdb.DataCacheKey, tableName string) (sql.Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tableName = strings.ToLower(tableName)
	if c.tables == nil {
		return nil, false
	}

	tablesForKey, ok := c.tables[key]
	if !ok {
		return nil, false
	}

	table, ok := tablesForKey[tableName]
	return table, ok
}

// CacheViews caches all views in a database for the cache key given
func (c *SessionCache) CacheViews(key doltdb.DataCacheKey, views []sql.ViewDefinition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.views == nil {
		c.views = make(map[doltdb.DataCacheKey]map[string]sql.ViewDefinition)
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		viewsForKey = make(map[string]sql.ViewDefinition)
		c.views[key] = viewsForKey
	}

	for i := range views {
		viewName := strings.ToLower(views[i].Name)
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
func (c *SessionCache) GetCachedViewDefinition(key doltdb.DataCacheKey, viewName string) (sql.ViewDefinition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	viewName = strings.ToLower(viewName)
	if c.views == nil {
		return sql.ViewDefinition{}, false
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		return sql.ViewDefinition{}, false
	}

	table, ok := viewsForKey[viewName]
	return table, ok
}
