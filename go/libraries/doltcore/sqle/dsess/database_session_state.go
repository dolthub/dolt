// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type InitialDbState struct {
	Db          sql.Database
	HeadCommit  *doltdb.Commit
	ReadOnly    bool
	WorkingSet  *doltdb.WorkingSet
	DbData      env.DbData
	ReadReplica *env.Remote
	Remotes     map[string]env.Remote
	Branches    map[string]env.BranchConfig
	Backups     map[string]env.Remote

	// If err is set, this InitialDbState is partially invalid, but may be
	// usable to initialize a database at a revision specifier, for
	// example. Adding this InitialDbState to a session will return this
	// error.
	Err error
}

type DatabaseSessionState struct {
	dbName       string
	headCommit   *doltdb.Commit
	headRoot     *doltdb.RootValue
	WorkingSet   *doltdb.WorkingSet
	dbData       env.DbData
	WriteSession writer.WriteSession
	globalState  globalstate.GlobalState
	readOnly     bool
	dirty        bool
	readReplica  *env.Remote
	tmpFileDir   string

	TblStats map[string]sql.TableStatistics

	// cache of indexes
	indexCache map[doltdb.DataCacheKey]map[string][]sql.Index

	// cache of tables
	tableCache map[doltdb.DataCacheKey]map[string]sql.Table

	// cache of views
	viewCache map[doltdb.DataCacheKey]map[string]string

	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
}

func (d DatabaseSessionState) GetRoots() doltdb.Roots {
	if d.WorkingSet == nil {
		return doltdb.Roots{
			Head:    d.headRoot,
			Working: d.headRoot,
			Staged:  d.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.WorkingSet.WorkingRoot(),
		Staged:  d.WorkingSet.StagedRoot(),
	}
}

func (d *DatabaseSessionState) CacheTableIndexes(key doltdb.DataCacheKey, table string, indexes []sql.Index) {
	table = strings.ToLower(table)

	if d.indexCache == nil {
		d.indexCache = make(map[doltdb.DataCacheKey]map[string][]sql.Index)
	}

	tableIndexes, ok := d.indexCache[key]
	if !ok {
		tableIndexes = make(map[string][]sql.Index)
		d.indexCache[key] = tableIndexes
	}

	tableIndexes[table] = indexes
}

func (d *DatabaseSessionState) GetTableIndexesCache(key doltdb.DataCacheKey, table string) ([]sql.Index, bool) {
	table = strings.ToLower(table)

	if d.indexCache == nil {
		return nil, false
	}

	tableIndexes, ok := d.indexCache[key]
	if !ok {
		return nil, false
	}

	indexes, ok := tableIndexes[table]
	return indexes, ok
}

func (d *DatabaseSessionState) CacheTable(key doltdb.DataCacheKey, tableName string, table sql.Table) {
	tableName = strings.ToLower(tableName)

	if d.tableCache == nil {
		d.tableCache = make(map[doltdb.DataCacheKey]map[string]sql.Table)
	}

	tablesForKey, ok := d.tableCache[key]
	if !ok {
		tablesForKey = make(map[string]sql.Table)
		d.tableCache[key] = tablesForKey
	}

	tablesForKey[tableName] = table
}

func (d *DatabaseSessionState) GetCachedTable(key doltdb.DataCacheKey, tableName string) (sql.Table, bool) {
	tableName = strings.ToLower(tableName)

	if d.tableCache == nil {
		return nil, false
	}

	tablesForKey, ok := d.tableCache[key]
	if !ok {
		return nil, false
	}

	table, ok := tablesForKey[tableName]
	return table, ok
}

func (d *DatabaseSessionState) CacheViews(key doltdb.DataCacheKey, viewNames []string, viewDefs []string) {
	if d.viewCache == nil {
		d.viewCache = make(map[doltdb.DataCacheKey]map[string]string)
	}

	viewsForKey, ok := d.viewCache[key]
	if !ok {
		viewsForKey = make(map[string]string)
		d.viewCache[key] = viewsForKey
	}

	for i := range viewNames {
		viewName := strings.ToLower(viewNames[i])
		viewsForKey[viewName] = viewDefs[i]
	}
}

func (d *DatabaseSessionState) ViewsCached(key doltdb.DataCacheKey) bool {
	if d.viewCache == nil {
		return false
	}

	_, ok := d.viewCache[key]
	return ok
}

func (d *DatabaseSessionState) GetCachedView(key doltdb.DataCacheKey, viewName string) (string, bool) {
	viewName = strings.ToLower(viewName)

	if d.viewCache == nil {
		return "", false
	}

	viewsForKey, ok := d.viewCache[key]
	if !ok {
		return "", false
	}

	table, ok := viewsForKey[viewName]
	return table, ok
}

func (d DatabaseSessionState) EditOpts() editor.Options {
	return d.WriteSession.GetOptions()
}
