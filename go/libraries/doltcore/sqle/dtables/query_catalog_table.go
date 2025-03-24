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
	"io"

	"github.com/google/uuid"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
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

func savedQueryFromKVProlly(id string, value val.Tuple) (SavedQuery, error) {
	orderVal, ok := catalogVd.GetUint64(0, value)
	if !ok {
		orderVal = 0
	}
	nameVal, ok := catalogVd.GetString(1, value)
	if !ok {
		nameVal = ""
	}
	queryVal, ok := catalogVd.GetString(2, value)
	if !ok {
		nameVal = ""
	}
	descVal, ok := catalogVd.GetString(3, value)
	if !ok {
		descVal = ""
	}

	return SavedQuery{
		ID:          id,
		Name:        nameVal,
		Query:       queryVal,
		Description: descVal,
		Order:       orderVal,
	}, nil
}

func savedQueryFromKVNoms(id string, valTuple types.Tuple) (SavedQuery, error) {
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

func (sq SavedQuery) asRow(nbf *types.NomsBinFormat) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	taggedVals[schema.QueryCatalogIdTag] = types.String(sq.ID)
	taggedVals[schema.QueryCatalogOrderTag] = types.Uint(sq.Order)
	taggedVals[schema.QueryCatalogNameTag] = types.String(sq.Name)
	taggedVals[schema.QueryCatalogQueryTag] = types.String(sq.Query)
	taggedVals[schema.QueryCatalogDescriptionTag] = types.String(sq.Description)

	return row.New(nbf, DoltQueryCatalogSchema, taggedVals)
}

var DoltQueryCatalogSchema = schema.MustSchemaFromCols(queryCatalogCols)

// system tables do not contain addressable columns, and do not require nodestore access.
var catalogKd = DoltQueryCatalogSchema.GetKeyDescriptor(nil)
var catalogVd = DoltQueryCatalogSchema.GetValueDescriptor(nil)

// Creates the query catalog table if it doesn't exist.
func createQueryCatalogIfNotExists(ctx context.Context, root doltdb.RootValue) (doltdb.RootValue, error) {
	_, ok, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName})
	if err != nil {
		return nil, err
	}

	if !ok {
		return doltdb.CreateEmptyTable(ctx, root, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName}, DoltQueryCatalogSchema)
	}

	return root, nil
}

// NewQueryCatalogEntryWithRandID saves a new entry in the query catalog table and returns the new root value. An ID will be
// chosen automatically.
func NewQueryCatalogEntryWithRandID(ctx context.Context, root doltdb.RootValue, name, query, description string) (SavedQuery, doltdb.RootValue, error) {
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
func NewQueryCatalogEntryWithNameAsID(ctx context.Context, root doltdb.RootValue, name, query, description string) (SavedQuery, doltdb.RootValue, error) {
	return newQueryCatalogEntry(ctx, root, name, name, query, description)
}

func newQueryCatalogEntry(ctx context.Context, root doltdb.RootValue, id, name, query, description string) (SavedQuery, doltdb.RootValue, error) {
	root, err := createQueryCatalogIfNotExists(ctx, root)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName})
	if err != nil {
		return SavedQuery{}, nil, err
	}

	var sq SavedQuery
	var newTable *doltdb.Table
	if types.IsFormat_DOLT(tbl.Format()) {
		sq, newTable, err = newQueryCatalogEntryProlly(ctx, tbl, id, name, query, description)
	} else {
		sq, newTable, err = newQueryCatalogEntryNoms(ctx, tbl, id, name, query, description)
	}
	if err != nil {
		return SavedQuery{}, nil, err
	}

	root, err = root.PutTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName}, newTable)

	if err != nil {
		return SavedQuery{}, nil, err
	}

	return sq, root, err
}

func newQueryCatalogEntryNoms(ctx context.Context, tbl *doltdb.Table, id, name, query, description string) (SavedQuery, *doltdb.Table, error) {
	data, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	order := getMaxQueryOrderNoms(data, ctx) + 1
	existingSQ, err := retrieveFromQueryCatalogNoms(ctx, tbl, id)

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

	r, err := sq.asRow(tbl.Format())
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

	return sq, newTable, nil
}

func newQueryCatalogEntryProlly(ctx context.Context, tbl *doltdb.Table, id, name, query, description string) (SavedQuery, *doltdb.Table, error) {
	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return SavedQuery{}, nil, err
	}
	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	existingSQ, err := retrieveFromQueryCatalogProlly(ctx, tbl, id)
	if err != nil && !ErrQueryNotFound.Is(err) {
		return SavedQuery{}, nil, err
	}

	var order uint64
	if ErrQueryNotFound.Is(err) {
		order, err = getMaxQueryOrderProlly(ctx, m)
		if err != nil {
			return SavedQuery{}, nil, err
		}
		order++
	} else {
		order = existingSQ.Order
	}

	kb := val.NewTupleBuilder(catalogKd, m.NodeStore())
	vb := val.NewTupleBuilder(catalogVd, m.NodeStore())
	kb.PutString(0, id)
	k, _ := kb.Build(m.Pool())

	vb.PutUint64(0, order)
	vb.PutString(1, name)
	vb.PutString(2, query)
	vb.PutString(3, description)
	v, _ := vb.Build(m.Pool())

	mut := m.Mutate()
	err = mut.Put(ctx, k, v)
	if err != nil {
		return SavedQuery{}, nil, err
	}
	m, err = mut.Map(ctx)
	if err != nil {
		return SavedQuery{}, nil, err
	}
	idx = durable.IndexFromProllyMap(m)

	tbl, err = tbl.UpdateRows(ctx, idx)
	if err != nil {
		return SavedQuery{}, nil, err
	}

	return SavedQuery{
		ID:          id,
		Name:        name,
		Query:       query,
		Description: description,
		Order:       order,
	}, tbl, nil
}

func RetrieveFromQueryCatalog(ctx context.Context, root doltdb.RootValue, id string) (SavedQuery, error) {
	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.DoltQueryCatalogTableName})

	if err != nil {
		return SavedQuery{}, err
	} else if !ok {
		return SavedQuery{}, doltdb.ErrTableNotFound
	}

	if types.IsFormat_DOLT(tbl.Format()) {
		return retrieveFromQueryCatalogProlly(ctx, tbl, id)
	}

	return retrieveFromQueryCatalogNoms(ctx, tbl, id)
}

func retrieveFromQueryCatalogProlly(ctx context.Context, tbl *doltdb.Table, id string) (SavedQuery, error) {
	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return SavedQuery{}, err
	}

	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return SavedQuery{}, err
	}

	kb := val.NewTupleBuilder(catalogKd, m.NodeStore())
	kb.PutString(0, id)
	k, _ := kb.Build(m.Pool())
	var value val.Tuple
	_ = m.Get(ctx, k, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	if value == nil {
		return SavedQuery{}, ErrQueryNotFound.New(id)
	}

	return savedQueryFromKVProlly(id, value)
}

func retrieveFromQueryCatalogNoms(ctx context.Context, tbl *doltdb.Table, id string) (SavedQuery, error) {
	m, err := tbl.GetNomsRowData(ctx)

	if err != nil {
		return SavedQuery{}, err
	}

	k, err := types.NewTuple(tbl.Format(), types.Uint(schema.QueryCatalogIdTag), types.String(id))

	if err != nil {
		return SavedQuery{}, err
	}

	val, ok, err := m.MaybeGet(ctx, k)

	if err != nil {
		return SavedQuery{}, err
	} else if !ok {
		return SavedQuery{}, ErrQueryNotFound.New(id)
	}

	return savedQueryFromKVNoms(id, val.(types.Tuple))
}

// Returns the largest order entry in the catalog
func getMaxQueryOrderNoms(data types.Map, ctx context.Context) uint64 {
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

func getMaxQueryOrderProlly(ctx context.Context, data prolly.Map) (uint64, error) {
	itr, err := data.IterAll(ctx)
	if err != nil {
		return 0, err
	}

	maxOrder := uint64(0)
	for {
		_, v, err := itr.Next(ctx)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if err == io.EOF {
			return maxOrder, nil
		}
		order, ok := catalogVd.GetUint64(0, v)
		if ok {
			if order > maxOrder {
				maxOrder = order
			}
		}
	}
}
