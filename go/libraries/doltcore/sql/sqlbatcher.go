// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// SqlBatcher knows how to efficiently batch insert / update statements, e.g. when doing a SQL import. It does this by
// using a single MapEditor per table that isn't persisted until Commit is called.
type SqlBatcher struct {
	// The database we are editing
	db *doltdb.DoltDB
	// The root value we are editing
	root *doltdb.RootValue
	// The set of tables under edit
	tables map[string]*doltdb.Table
	// The schemas of tables under edit
	schemas map[string]schema.Schema
	// The row data for tables being edited
	rowData map[string]types.Map
	// The editors applying updates to the tables
	editors map[string]*types.MapEditor
	// The hashes of primary keys being inserted to the tables
	hashes map[string]map[hash.Hash]bool
}

// Returns a new SqlBatcher for the given environment and root value.
func NewSqlBatcher(db *doltdb.DoltDB, root *doltdb.RootValue) *SqlBatcher {
	batcher := &SqlBatcher{
		db:      db,
		root:    root,
	}
	batcher.resetState()
	return batcher
}

// Updates this batcher with a new root value.  If there are outstanding edits, returns an error.
func (b *SqlBatcher) UpdateRoot(root *doltdb.RootValue) error {
	if b.isDirty() {
		return errors.New("UpdateRoot called with outstanding edits")
	}
	b.root = root

	// resetting the state shouldn't be necessary here because of the isDirty check, but if the client chooses to ignore
	// the returned error, we'll at least have a clean state going forward
	b.resetState()
	return nil
}

// isDirty returns whether there are outstanding edits that haven't been committed.
func (b *SqlBatcher) isDirty() bool {
	return len(b.editors) > 0
}

// resetState flushes the cache of outstanding edits and other data
func (b *SqlBatcher) resetState() {
	b.tables = make(map[string]*doltdb.Table)
	b.schemas = make(map[string]schema.Schema)
	b.rowData = make(map[string]types.Map)
	b.editors = make(map[string]*types.MapEditor)
	b.hashes = make(map[string]map[hash.Hash]bool)
}

type InsertOptions struct {
	// Whether to silently replace any existing rows with the same primary key as rows inserted
	Replace bool
}

type BatchInsertResult struct {
	RowInserted  bool
	RowUpdated   bool
}

func (b *SqlBatcher) Insert(ctx context.Context, tableName string, r row.Row, opt InsertOptions) (*BatchInsertResult, error) {
	sch, err := b.GetSchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	rowData, err := b.getRowData(ctx, tableName)
	if err != nil {
		return nil, err
	}

	ed, err := b.getEditor(ctx, tableName)
	if err != nil {
		return nil, err
	}

	key := r.NomsMapKey(sch).Value(ctx)

	rowExists := rowData.Get(ctx, key) != nil
	hashes := b.getHashes(ctx, tableName)
	rowAlreadyTouched := hashes[key.Hash(b.root.VRW().Format())]

	if rowExists || rowAlreadyTouched {
		if !opt.Replace {
			return nil, fmt.Errorf("Duplicate primary key: '%v'", getPrimaryKeyString(r, sch))
		}
	}

	ed.Set(key, r.NomsMapValue(sch))
	hashes[key.Hash(b.root.VRW().Format())] = true

	return &BatchInsertResult{RowInserted: !rowExists, RowUpdated: rowExists || rowAlreadyTouched}, nil
}

// GetTable returns the table with the name given. This method is offered because reading the table from the root value
// is relatively expensive, and SqlBatcher caches Tables to avoid the overhead.
func (b *SqlBatcher) GetTable(ctx context.Context, tableName string) (*doltdb.Table, error) {
	if table, ok := b.tables[tableName]; ok {
		return table, nil
	}

	if !b.root.HasTable(ctx, tableName) {
		return nil, fmt.Errorf("Unknown table %v", tableName)
	}

	table, _ := b.root.GetTable(ctx, tableName)
	b.tables[tableName] = table
	return table, nil
}

// GetSchema returns the schema for the table name given. This method is offered because reading the schema from disk
// is actually relatively expensive -- SqlBatcher caches the schema values per table to avoid the overhead.
func (b *SqlBatcher) GetSchema(ctx context.Context, tableName string) (schema.Schema, error) {
	if schema, ok := b.schemas[tableName]; ok {
		return schema, nil
	}

	table, err := b.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	sch := table.GetSchema(ctx)
	b.schemas[tableName] = sch
	return sch,  nil
}

func (b *SqlBatcher) getEditor(ctx context.Context, tableName string) (*types.MapEditor, error) {
	if ed, ok := b.editors[tableName]; ok {
		return ed, nil
	}

	rowData, err := b.getRowData(ctx, tableName)
	if err != nil {
		return nil, err
	}

	ed := rowData.Edit()
	b.editors[tableName] = ed
	return ed, nil
}

func (b *SqlBatcher) getRowData(ctx context.Context, tableName string) (types.Map, error) {
	if rowData, ok := b.rowData[tableName]; ok {
		return rowData, nil
	}

	table, err := b.GetTable(ctx, tableName)
	if err != nil {
		return types.EmptyMap, err
	}

	rowData := table.GetRowData(ctx)
	b.rowData[tableName] = rowData
	return rowData, nil
}

func (b *SqlBatcher) getHashes(ctx context.Context, tableName string) map[hash.Hash]bool {
	if hashes, ok := b.hashes[tableName]; ok {
		return hashes
	}

	hashes := make(map[hash.Hash]bool)
	b.hashes[tableName] = hashes
	return hashes
}

func (b *SqlBatcher) Update(r row.Row) {

}

// Commit writes a new root value for every table under edit and returns the new root value. Tables are written in an
// arbitrary order.
func (b *SqlBatcher) Commit(ctx context.Context) (*doltdb.RootValue, error) {
	root := b.root

	for tableName, ed := range b.editors {
		newMap := ed.Map(ctx)
		table := b.tables[tableName]
		table = table.UpdateRows(ctx, newMap)
		root = root.PutTable(ctx, b.db, tableName, table)
	}

	b.root = root
	b.resetState()

	return root, nil
}