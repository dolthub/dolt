// Copyright 2023 Dolthub, Inc.
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

package statspro

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

// GetLatestTable will get the WORKING root table for the current database/branch
func GetLatestTable(ctx *sql.Context, tableName string, sqlDb sql.Database) (*sqle.DoltTable, *doltdb.Table, error) {
	var db sqle.Database
	switch d := sqlDb.(type) {
	case sqle.Database:
		db = d
	case sqle.ReadReplicaDatabase:
		db = d.Database
	default:
		return nil, nil, fmt.Errorf("expected sqle.Database, found %T", sqlDb)
	}
	sqlTable, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("statistics refresh error: table not found %s", tableName)
	}

	var dTab *doltdb.Table
	var sqleTable *sqle.DoltTable
	switch t := sqlTable.(type) {
	case *sqle.AlterableDoltTable:
		sqleTable = t.DoltTable
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		sqleTable = t.DoltTable
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		sqleTable = t
		dTab, err = t.DoltTable(ctx)
	default:
		err = fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
	}
	if err != nil {
		return nil, nil, err
	}
	return sqleTable, dTab, nil
}

type templateCacheKey struct {
	h       hash.Hash
	idxName string
}

func (k templateCacheKey) String() string {
	return k.idxName + "/" + k.h.String()[:5]
}

func (sc *StatsCoord) getTemplate(ctx *sql.Context, sqlTable *sqle.DoltTable, sqlIdx sql.Index) (templateCacheKey, stats.Statistic, error) {
	schHash, _, err := sqlTable.IndexCacheKey(ctx)
	key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
	if template, ok := sc.kv.GetTemplate(key); ok {
		return key, template, nil
	}
	fds, colset, err := stats.IndexFds(strings.ToLower(sqlTable.Name()), sqlTable.Schema(), sqlIdx)
	if err != nil {
		return templateCacheKey{}, stats.Statistic{}, err
	}

	var class sql.IndexClass
	switch {
	case sqlIdx.IsSpatial():
		class = sql.IndexClassSpatial
	case sqlIdx.IsFullText():
		class = sql.IndexClassFulltext
	default:
		class = sql.IndexClassDefault
	}

	var types []sql.Type
	for _, cet := range sqlIdx.ColumnExpressionTypes() {
		types = append(types, cet.Type)
	}

	tablePrefix := sqlTable.Name() + "."
	cols := make([]string, len(sqlIdx.Expressions()))
	for i, c := range sqlIdx.Expressions() {
		cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
	}

	template := stats.Statistic{
		Qual:     sql.NewStatQualifier("", "", sqlTable.Name(), sqlIdx.ID()),
		Cols:     cols,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	}

	// We put template twice, once for schema changes with no data
	// changes (here), and once when we put chunks to avoid GC dropping
	// templates before the finalize job.
	sc.kv.PutTemplate(key, template)

	return key, template, nil
}
