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

package doltdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

// TableEditSession handles all edit operations on a table that may also update other tables. Serves as coordination
// for SessionedTableEditors.
type TableEditSession struct {
	Props TableEditSessionProps

	root       *RootValue
	tables     map[string]*SessionedTableEditor
	writeMutex *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs
}

// TableEditSessionProps are properties that define different functionality for the TableEditSession.
type TableEditSessionProps struct {
	ForeignKeyChecksDisabled bool // If disabled, then all foreign key checks and updates are ignored
}

// CreateTableEditSession creates and returns a TableEditSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func CreateTableEditSession(root *RootValue, props TableEditSessionProps) *TableEditSession {
	return &TableEditSession{
		Props:      props,
		root:       root,
		tables:     make(map[string]*SessionedTableEditor),
		writeMutex: &sync.RWMutex{},
	}
}

// GetTableEditor returns a SessionedTableEditor for the given table. If a schema is provided and it does not match the one
// that is used for currently open editors (if any), then those editors will reload the table from the root.
func (tes *TableEditSession) GetTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (*SessionedTableEditor, error) {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	return tes.getTableEditor(ctx, tableName, tableSch)
}

// GetRoot returns an updated root with all of the changed tables.
func (tes *TableEditSession) GetRoot(ctx context.Context) (*RootValue, error) {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	rootMutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(len(tes.tables))

	newRoot := tes.root
	var tableErr error
	var rootErr error
	for tableName, ste := range tes.tables {
		// we can run all of the Table calls concurrently as long as we guard updating the root
		go func(tableName string, ste *SessionedTableEditor) {
			defer wg.Done()
			updatedTable, err := ste.tableEditor.Table()
			// we lock immediately after doing the operation, since both error setting and root updating are guarded
			rootMutex.Lock()
			defer rootMutex.Unlock()
			if err != nil {
				if tableErr == nil {
					tableErr = err
				}
				return
			}
			newRoot, err = newRoot.PutTable(ctx, tableName, updatedTable)
			if err != nil && rootErr == nil {
				rootErr = err
			}
		}(tableName, ste)
	}
	wg.Wait()
	if tableErr != nil {
		return nil, tableErr
	}
	if rootErr != nil {
		return nil, rootErr
	}

	tes.root = newRoot
	return newRoot, nil
}

// SetRoot uses the given root to set all open table editors to the state as represented in the root. If any
// tables are removed in the root, but have open table editors, then the references to those are removed. If those
// removed table's editors are used after this, then the behavior is undefined.
func (tes *TableEditSession) SetRoot(ctx context.Context, root *RootValue) error {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	if root == nil {
		return fmt.Errorf("cannot set a TableEditSession's root to nil once it has been created")
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	tes.root = root

	for tableName, localTableEditor := range tes.tables {
		t, ok, err := root.GetTable(ctx, tableName)
		if err != nil {
			return err
		}
		if !ok { // table was removed in newer root
			delete(tes.tables, tableName)
			continue
		}
		tSch, err := t.GetSchema(ctx)
		if err != nil {
			return err
		}
		newTableEditor, err := NewTableEditor(ctx, t, tSch)
		if err != nil {
			return err
		}
		localTableEditor.tableEditor = newTableEditor
		localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
		err = tes.loadForeignKeys(ctx, localTableEditor)
		if err != nil {
			return err
		}
	}
	return nil
}

// getTableEditor is the inner implementation for GetTableEditor, allowing recursive calls
func (tes *TableEditSession) getTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (*SessionedTableEditor, error) {
	if tes.root == nil {
		return nil, fmt.Errorf("must call SetRoot before a table editor will be returned")
	}

	var t *Table
	var err error
	localTableEditor, ok := tes.tables[tableName]
	if ok {
		if tableSch == nil {
			return localTableEditor, nil
		} else if ok, err = schema.SchemasAreEqual(tableSch, localTableEditor.tableEditor.tSch); err == nil && ok {
			return localTableEditor, nil
		}
		// Any existing references to this localTableEditor should be preserved, so we just change the underlying values
		localTableEditor.referencedTables = nil
		localTableEditor.referencingTables = nil
	} else {
		localTableEditor = &SessionedTableEditor{
			tableEditSession:  tes,
			tableEditor:       nil,
			referencedTables:  nil,
			referencingTables: nil,
		}
		tes.tables[tableName] = localTableEditor
	}

	t, ok, err = tes.root.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unable to create table editor as `%s` is missing", tableName)
	}
	if tableSch == nil {
		tableSch, err = t.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	tableEditor, err := NewTableEditor(ctx, t, tableSch)
	if err != nil {
		return nil, err
	}
	localTableEditor.tableEditor = tableEditor
	if tes.Props.ForeignKeyChecksDisabled {
		return localTableEditor, nil
	}

	fkCollection, err := tes.root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
	err = tes.loadForeignKeys(ctx, localTableEditor)
	if err != nil {
		return nil, err
	}

	return localTableEditor, nil
}

// getTableEditorOrFail returns the table editor for this table or fails
func (tes *TableEditSession) getTableEditorOrFail(tableName string) (*SessionedTableEditor, error) {
	localTableEditor, ok := tes.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("unable to get table editor as `%s` is missing", tableName)
	}
	return localTableEditor, nil
}

// loadForeignKeys loads all tables mentioned in foreign keys for the given editor
func (tes *TableEditSession) loadForeignKeys(ctx context.Context, localTableEditor *SessionedTableEditor) error {
	// these are the tables that reference us, so we need to update them
	for _, foreignKey := range localTableEditor.referencingTables {
		_, err := tes.getTableEditor(ctx, foreignKey.TableName, nil)
		if err != nil {
			return err
		}
	}
	// these are the tables that we reference, so we need to refer to them
	for _, foreignKey := range localTableEditor.referencedTables {
		_, err := tes.getTableEditor(ctx, foreignKey.ReferencedTableName, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
