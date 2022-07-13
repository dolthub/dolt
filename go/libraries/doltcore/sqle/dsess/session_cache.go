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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
)

// SessionCache caches various pieces of expensive to compute information to speed up future lookups in the session.
// No methods are thread safe.
type SessionCache interface {
	// CacheTableIndexes caches all indexes for the table with the name given
	CacheTableIndexes(key doltdb.DataCacheKey, table string, indexes []sql.Index)
	// GetTableIndexesCache returns the cached index information for the table named, and whether the cache was present
	GetTableIndexesCache(key doltdb.DataCacheKey, table string) ([]sql.Index, bool)
	// CacheTable caches a sql.Table implementation for the table named
	CacheTable(key doltdb.DataCacheKey, tableName string, table sql.Table)
	// GetCachedTable returns the cached sql.Table for the table named, and whether the cache was present
	GetCachedTable(key doltdb.DataCacheKey, tableName string) (sql.Table, bool)
	// CacheViews caches all views in a database for the cache key given
	CacheViews(key doltdb.DataCacheKey, viewNames []string, viewDefs []string)
	// ViewsCached returns whether this cache has been initialized with the set of views yet
	ViewsCached(key doltdb.DataCacheKey) bool
	// GetCachedView returns the cached view named, and whether the cache was present
	GetCachedView(key doltdb.DataCacheKey, viewName string) (string, bool)
}

type sessionCache struct {
	indexes   map[doltdb.DataCacheKey]map[string][]sql.Index
	tables map[doltdb.DataCacheKey]map[string]sql.Table
	views  map[doltdb.DataCacheKey]map[string]string
}

func newSessionCache() *sessionCache {
	return &sessionCache{}
}

func (c *sessionCache) CacheTableIndexes(key doltdb.DataCacheKey, table string, indexes []sql.Index) {
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

func (c *sessionCache) GetTableIndexesCache(key doltdb.DataCacheKey, table string) ([]sql.Index, bool) {
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

func (c *sessionCache) CacheTable(key doltdb.DataCacheKey, tableName string, table sql.Table) {
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

func (c *sessionCache) GetCachedTable(key doltdb.DataCacheKey, tableName string) (sql.Table, bool) {
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

func (c *sessionCache) CacheViews(key doltdb.DataCacheKey, viewNames []string, viewDefs []string) {
	if c.views == nil {
		c.views = make(map[doltdb.DataCacheKey]map[string]string)
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		viewsForKey = make(map[string]string)
		c.views[key] = viewsForKey
	}

	for i := range viewNames {
		viewName := strings.ToLower(viewNames[i])
		viewsForKey[viewName] = viewDefs[i]
	}
}

func (c *sessionCache) ViewsCached(key doltdb.DataCacheKey) bool {
	if c.views == nil {
		return false
	}

	_, ok := c.views[key]
	return ok
}

func (c *sessionCache) GetCachedView(key doltdb.DataCacheKey, viewName string) (string, bool) {
	viewName = strings.ToLower(viewName)

	if c.views == nil {
		return "", false
	}

	viewsForKey, ok := c.views[key]
	if !ok {
		return "", false
	}

	table, ok := viewsForKey[viewName]
	return table, ok
}
