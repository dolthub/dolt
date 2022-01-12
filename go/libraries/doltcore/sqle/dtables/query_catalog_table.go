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

package dtables

import (
	"context"

	"github.com/google/uuid"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var queryCatalogCols = schema.NewColCollection(
	// QueryCatalogIdCol is the name of the primary key column of the query catalog table
	schema.NewColumn(doltdb.QueryCatalogIdCol, schema.QueryCatalogIdTag, types.StringKind, true, schema.NotNullConstraint{}),
	// QueryCatalogOrderCol is the column containing the order of the queries in the catalog
	schema.NewColumn(doltdb.QueryCatalogOrderCol, schema.QueryCatalogOrderTag, types.UintKind, false, schema.NotNullConstraint{}),
	// QueryCatalogNameCol is the name of the column containing the name of a query in the catalog
	// TODO: parser won't handle a reserved word here, but it should. Only an issue for create table statements.
	schema.NewColumn(doltdb.QueryCatalogNameCol, schema.QueryCatalogNameTag, types.StringKind, false),
	// QueryCatalogQueryCol is the name of the column containing the query of a catalog entry
	schema.NewColumn(doltdb.QueryCatalogQueryCol, schema.QueryCatalogQueryTag, types.StringKind, false),
	// QueryCatalogDescriptionCol is the name of the column containing the description of a query in the catalog
	schema.NewColumn(doltdb.QueryCatalogDescriptionCol, schema.QueryCatalogDescriptionTag, types.StringKind, false),
)

var ErrQueryNotFound = errors.NewKind("Query '%s' not found")

type SavedQuery struct {
	ID          string
	Name        string
	Query       string
	Description string
	Order       uint64
}

func savedQueryFromKV(id string, valTuple types.Tuple) (SavedQuery, error) {
	tv, err := row.ParseTaggedValues(valTuple)

	if err != nil {
		return SavedQuery{}, err
	}

	nameVal := tv.GetWithDefault(schema.QueryCatalogNameTag, types.String(""))
	queryVal := tv.GetWithDefault(schema.QueryCatalogQueryTag, types.String(""))
	descVal := tv.GetWithDefault(schema.QueryCatalogDescriptionTag, types.String(""))
	orderVal := tv.GetWithDefault(schema.QueryCatalogOrderTag, types.Uint(0))

	return SavedQuery{
		ID:          id,
		Name:        string(nameVal.(types.String)),
		Query:       string(queryVal.(types.String)),
		Description: string(descVal.(types.String)),
		Order:       uint64(orderVal.(types.Uint)),
	}, nil
}

func (sq SavedQuery) asRow() (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	taggedVals[schema.QueryCatalogIdTag] = types.String(sq.ID)
	taggedVals[schema.QueryCatalogOrderTag] = types.Uint(sq.Order)
	taggedVals[schema.QueryCatalogNameTag] = types.String(sq.Name)
	taggedVals[schema.QueryCatalogQueryTag] = types.String(sq.Query)
	taggedVals[schema.QueryCatalogDescriptionTag] = types.String(sq.Description)

	return row.New(types.Format_Default, DoltQueryCatalogSchema, taggedVals)
}

var DoltQueryCatalogSchema = schema.MustSchemaFromCols(queryCatalogCols)

// Creates the query catalog table if it doesn't exist.
func createQueryCatalogIfNotExists(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	_, ok, err := root.GetTable(ctx, doltdb.DoltQueryCatalogTableName)
	if err != nil {
		return nil, err
	}

	if !ok {
		return root.CreateEmptyTable(ctx, doltdb.DoltQueryCatalogTableName, DoltQueryCatalogSchema)
	}

	return root, nil
}

// NewQueryCatalogEntryWithRandID saves a new entry in the query catalog table and returns the new root value. An ID will be
// chosen automatically.
func NewQueryCatalogEntryWithRandID(ctx context.Context, root *doltdb.RootValue, name, query, description string) (SavedQuery, *doltdb.RootValue, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return SavedQuery{}, nil, err
	}

	// Use the last 12 hex digits of the uuid for the ID.
	uidStr := uid.String()
	id := uidStr[len(uidStr)-12:]

	return newQueryCatalogEntry(ctx, root, id, name, query, description)
}

// NewQueryCatalogEntryWithNameAsID saves an entry in the query catalog table and returns the new root value. If an
// entry with the given name is already present, it will be overwritten.
func NewQueryCatalogEntryWithNameAsID(ctx context.Context, root *doltdb.RootValue, name, query, description string) (SavedQuery, *doltdb.RootValue, error) {
	return newQueryCatalogEntry(ctx, root, name, name, query, description)
}

func newQueryCatalogEntry(ctx context.Context, root *doltdb.RootValue, id, name, query, description string) (SavedQuery, *doltdb.RootValue, error) {
	root, err := createQueryCatalogIfNotExists(ctx, root)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	tbl, _, err := root.GetTable(ctx, doltdb.DoltQueryCatalogTableName)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	data, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	order := getMaxQueryOrder(data, ctx) + 1
	existingSQ, err := RetrieveFromQueryCatalog(ctx, root, id)

	if err != nil {
		if !ErrQueryNotFound.Is(err) {
			return SavedQuery{}, nil, err
		}
	} else {
		order = existingSQ.Order
	}

	sq := SavedQuery{
		ID:          id,
		Name:        name,
		Query:       query,
		Description: description,
		Order:       order,
	}

	r, err := sq.asRow()
	if err != nil {
		return SavedQuery{}, nil, err
	}

	me := data.Edit()
	me.Set(r.NomsMapKey(DoltQueryCatalogSchema), r.NomsMapValue(DoltQueryCatalogSchema))

	updatedTable, err := me.Map(ctx)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	newTable, err := tbl.UpdateNomsRows(ctx, updatedTable)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	root, err = root.PutTable(ctx, doltdb.DoltQueryCatalogTableName, newTable)

	if err != nil {
		return SavedQuery{}, nil, err
	}

	return sq, root, err
}

func RetrieveFromQueryCatalog(ctx context.Context, root *doltdb.RootValue, id string) (SavedQuery, error) {
	tbl, ok, err := root.GetTable(ctx, doltdb.DoltQueryCatalogTableName)

	if err != nil {
		return SavedQuery{}, err
	} else if !ok {
		return SavedQuery{}, doltdb.ErrTableNotFound
	}

	m, err := tbl.GetNomsRowData(ctx)

	if err != nil {
		return SavedQuery{}, err
	}

	k, err := types.NewTuple(root.VRW().Format(), types.Uint(schema.QueryCatalogIdTag), types.String(id))

	if err != nil {
		return SavedQuery{}, err
	}

	val, ok, err := m.MaybeGet(ctx, k)

	if err != nil {
		return SavedQuery{}, err
	} else if !ok {
		return SavedQuery{}, ErrQueryNotFound.New(id)
	}

	return savedQueryFromKV(id, val.(types.Tuple))
}

// Returns the largest order entry in the catalog
func getMaxQueryOrder(data types.Map, ctx context.Context) uint64 {
	maxOrder := uint64(0)
	data.IterAll(ctx, func(key, value types.Value) error {
		r, _ := row.FromNoms(DoltQueryCatalogSchema, key.(types.Tuple), value.(types.Tuple))
		orderVal, ok := r.GetColVal(schema.QueryCatalogOrderTag)
		if ok {
			order := uint64(orderVal.(types.Uint))
			if order > maxOrder {
				maxOrder = order
			}
		}
		return nil
	})
	return maxOrder
}
