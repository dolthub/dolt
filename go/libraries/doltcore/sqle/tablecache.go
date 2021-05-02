// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// TableCache is a caches for sql.Tables.
// Caching schema fetches is a meaningful perf win.
type TableCache interface {
	// Get returns a sql.Table from the caches, if it exists for |root|.
	Get(tableName string, root *doltdb.RootValue) (sql.Table, bool)

	// Put stores a copy of |tbl| corresponding to |root|.
	Put(tableName string, root *doltdb.RootValue, tbl sql.Table)

	// AllForRoot retrieves all tables from the caches corresponding to |root|.
	AllForRoot(root *doltdb.RootValue) (map[string]sql.Table, bool)

	// Clear removes all entries from the cache.
	Clear()
}

func newTableCache() TableCache {
	return tableCache{
		mu:     &sync.Mutex{},
		tables: make(map[*doltdb.RootValue]map[string]sql.Table),
	}
}

type tableCache struct {
	mu     *sync.Mutex
	tables map[*doltdb.RootValue]map[string]sql.Table
}

var _ TableCache = tableCache{}

func (tc tableCache) Get(tableName string, root *doltdb.RootValue) (sql.Table, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if !ok {
		return nil, false
	}

	tbl, ok := tablesForRoot[tableName]

	return tbl, ok
}

func (tc tableCache) Put(tableName string, root *doltdb.RootValue, tbl sql.Table) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if !ok {
		tablesForRoot = make(map[string]sql.Table)
		tc.tables[root] = tablesForRoot
	}

	tablesForRoot[tableName] = tbl
}

func (tc tableCache) AllForRoot(root *doltdb.RootValue) (map[string]sql.Table, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if ok {
		copyOf := make(map[string]sql.Table, len(tablesForRoot))
		for name, tbl := range tablesForRoot {
			copyOf[name] = tbl
		}

		return copyOf, true
	}

	return nil, false
}

func (tc tableCache) Clear() {
	for rt := range tc.tables {
		delete(tc.tables, rt)
	}
}
