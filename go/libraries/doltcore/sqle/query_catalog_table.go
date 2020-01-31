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
	// DoltQueryCatalogTableName is the name of the query catalog table
	DoltQueryCatalogTableName = "dolt_query_catalog"

	// QueryCatalogIdCol is the name of the primary key column of the query catalog table
	QueryCatalogIdCol = "id"

	// QueryCatalogOrderCol is the column containing the order of the queries in the catalog
	QueryCatalogOrderCol = "order"

	// QueryCatalogNameCol is the name of the column containing the name of a query in the catalog
	QueryCatalogNameCol = "name"

	// QueryCatalogQueryCol is the name of the column containing the query of a catalog entry
	// TODO: parser won't handle a reserved word here, but it should. Only an issue for create table statements.
	QueryCatalogQueryCol = "query"

	// QueryCatalogDescriptionCol is the name of the column containing the description of a query in the catalog
	QueryCatalogDescriptionCol = "description"
)

const (
	queryCatalogIdTag uint64 = iota
	queryCatalogOrderTag
	queryCatalogNameTag
	queryCatalogQueryTag
	queryCatalogDescriptionTag
)

var queryCatalogCols, _ = schema.NewColCollection(
	schema.NewColumn(QueryCatalogIdCol, queryCatalogIdTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(QueryCatalogOrderCol, queryCatalogOrderTag, types.UintKind, false, schema.NotNullConstraint{}),
	schema.NewColumn(QueryCatalogNameCol, queryCatalogNameTag, types.StringKind, false),
	schema.NewColumn(QueryCatalogQueryCol, queryCatalogQueryTag, types.StringKind, false),
	schema.NewColumn(QueryCatalogDescriptionCol, queryCatalogDescriptionTag, types.StringKind, false),
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

	order := getMaxQueryOrder(data, ctx) + 1

	// Use the last 12 hex digits of the uuid for the ID.
	id := uid.String()[24:]
	r, err := newQueryCatalogRow(id, order, name, query, description)
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

// Returns the largest order entry in the catalog
func getMaxQueryOrder(data types.Map, ctx context.Context) uint {
	maxOrder := uint(0)
	data.IterAll(ctx, func(key, value types.Value) error {
		r, _ := row.FromNoms(queryCatalogSch, key.(types.Tuple), value.(types.Tuple))
		orderVal, ok := r.GetColVal(1)
		if ok {
			order := uint(orderVal.(types.Uint))
			if order > maxOrder {
				maxOrder = order
			}
		}
		return nil
	})
	return maxOrder
}

func newQueryCatalogRow(id string, order uint, name, query, description string) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	taggedVals[queryCatalogIdTag] = types.String(id)
	taggedVals[queryCatalogOrderTag] = types.Uint(order)
	taggedVals[queryCatalogNameTag] = types.String(name)
	taggedVals[queryCatalogQueryTag] = types.String(query)
	taggedVals[queryCatalogDescriptionTag] = types.String(description)
	return row.New(types.Format_Default, queryCatalogSch, taggedVals)
}