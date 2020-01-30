// Copyright 2020 Liquidata, Inc.
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
	"context"
	"github.com/google/uuid"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	// DoltHistoryTablePrefix is the name prefix for each history table
	DoltQueryCatalogTableName = "dolt_query_catalog"

	// CommitHashCol is the name of the column containing the commit hash in the result set
	QueryCatalogIdCol = "id"

	// CommitterCol is the name of the column containing the committer in the result set
	QueryCatalogNameCol = "name"

	// CommitterCol is the name of the column containing the committer in the result set
	// TODO: parser won't handle a reserved word here, but it should
	QueryCatalogQueryCol = "query"

	// CommitDateCol is the name of the column containing the commit date in the result set
	QueryCatalogDescriptionCol = "description"
)

var queryCatalogCols, _ = schema.NewColCollection(
	schema.NewColumn(QueryCatalogIdCol, 0, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(QueryCatalogNameCol, 1, types.StringKind, false),
	schema.NewColumn(QueryCatalogQueryCol, 2, types.StringKind, false),
	schema.NewColumn(QueryCatalogDescriptionCol, 3, types.StringKind, false),
)

var queryCatalogSch = schema.SchemaFromCols(queryCatalogCols)

// Creates the query catalog table if it doesn't exist.
func createQueryCatalogIfNotExists(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	_, ok, err := root.GetTable(ctx, DoltQueryCatalogTableName)
	if err != nil {
		return nil, err
	}

	if !ok {
		return root.CreateEmptyTable(ctx, DoltQueryCatalogTableName, queryCatalogSch)
	}

	return root, nil
}

// Saves a new entry in the query catalog table and returns the new root value. An ID will be chosen automatically.
func NewQueryCatalogEntry(ctx context.Context, root *doltdb.RootValue, name, query, description string) (*doltdb.RootValue, error) {
	root, err := createQueryCatalogIfNotExists(ctx, root)
	if err != nil {
		return nil, err
	}

	tbl, _, err := root.GetTable(ctx, DoltQueryCatalogTableName)
	if err != nil {
		return nil, err
	}

	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	data, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	// Use the last 12 hex digits of the uuid for the ID.
	id := uid.String()[24:]
	r, err := newQueryCatalogRow(id, name, query, description)
	if err != nil {
		return nil, err
	}

	me := data.Edit()
	me.Set(r.NomsMapKey(queryCatalogSch), r.NomsMapValue(queryCatalogSch))

	updatedTable, err := me.Map(ctx)
	if err != nil {
		return nil, err
	}

	newTable, err := tbl.UpdateRows(ctx, updatedTable)
	if err != nil {
		return nil, err
	}

	return doltdb.PutTable(ctx, root, root.VRW(), DoltQueryCatalogTableName, newTable)
}

func newQueryCatalogRow(id, name, query, description string) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	taggedVals[0] = types.String(id)
	taggedVals[1] = types.String(name)
	taggedVals[2] = types.String(query)
	taggedVals[3] = types.String(description)
	return row.New(types.Format_Default, queryCatalogSch, taggedVals)
}