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

package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type WriteSession interface {
	GetTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (editor.TableEditor, error)
	SetRoot(ctx context.Context, root *doltdb.RootValue) error
	UpdateRoot(ctx context.Context, updatingFunc func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error)) error
	Flush(ctx context.Context) (*doltdb.RootValue, error)

	GetOptions() editor.Options
	SetOptions(opts editor.Options)
}

// tableEditSession handles all edit operations on a table that may also update other tables. Serves as coordination
// for SessionedTableEditors.
type tableEditSession struct {
	opts editor.Options

	root       *doltdb.RootValue
	tables     map[string]*sessionedTableEditor
	writeMutex *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs
}

var _ WriteSession = &tableEditSession{}

func TestEditorOptions(vrw types.ValueReadWriter) editor.Options {
	return editor.Options{
		ForeignKeyChecksDisabled: false,
		Deaf:                     editor.NewInMemDeaf(vrw.Format()),
	}
}

// CreateTableEditSession creates and returns a tableEditSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func CreateTableEditSession(root *doltdb.RootValue, opts editor.Options) WriteSession {
	return &tableEditSession{
		opts:       opts,
		root:       root,
		tables:     make(map[string]*sessionedTableEditor),
		writeMutex: &sync.RWMutex{},
	}
}

// GetTableEditor returns a TableEditor for the given table. If a schema is provided and it does not match the one
// that is used for currently open editors (if any), then those editors will reload the table from the root.
func (tes *tableEditSession) GetTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (editor.TableEditor, error) {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	return tes.getTableEditor(ctx, tableName, tableSch)
}

// Flush returns an updated root with all of the changed tables.
func (tes *tableEditSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	return tes.flush(ctx)
}

// SetRoot uses the given root to set all open table editors to the state as represented in the root. If any
// tables are removed in the root, but have open table editors, then the references to those are removed. If those
// removed table's editors are used after this, then the behavior is undefined. This will lose any changes that have not
// been flushed. If the purpose is to add a new table, foreign key, etc. (using Flush followed up with SetRoot), then
// use UpdateRoot. Calling the two functions manually for the purposes of root modification may lead to race conditions.
func (tes *tableEditSession) SetRoot(ctx context.Context, root *doltdb.RootValue) error {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	return tes.setRoot(ctx, root)
}

// UpdateRoot takes in a function meant to update the root (whether that be updating a table's schema, adding a foreign
// key, etc.) and passes in the flushed root. The function may then safely modify the root, and return the modified root
// (assuming no errors). The tableEditSession will update itself in accordance with the newly returned root.
func (tes *tableEditSession) UpdateRoot(ctx context.Context, updatingFunc func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	tes.writeMutex.Lock()
	defer tes.writeMutex.Unlock()

	root, err := tes.flush(ctx)
	if err != nil {
		return err
	}
	newRoot, err := updatingFunc(ctx, root)
	if err != nil {
		return err
	}
	return tes.setRoot(ctx, newRoot)
}

func (tes *tableEditSession) GetOptions() editor.Options {
	return tes.opts
}

func (tes *tableEditSession) SetOptions(opts editor.Options) {
	tes.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (tes *tableEditSession) flush(ctx context.Context) (*doltdb.RootValue, error) {
	rootMutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(len(tes.tables))

	newRoot := tes.root
	var tableErr error
	var rootErr error
	for tableName, ste := range tes.tables {
		if !ste.HasEdits() {
			wg.Done()
			continue
		}

		// we can run all of the Table calls concurrently as long as we guard updating the root
		go func(tableName string, ste *sessionedTableEditor) {
			defer wg.Done()
			updatedTable, err := ste.tableEditor.Table(ctx)
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

// getTableEditor is the inner implementation for GetTableEditor, allowing recursive calls
func (tes *tableEditSession) getTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (*sessionedTableEditor, error) {
	if tes.root == nil {
		return nil, fmt.Errorf("must call SetRoot before a table editor will be returned")
	}

	var t *doltdb.Table
	var err error
	localTableEditor, ok := tes.tables[tableName]
	if ok {
		if tableSch == nil {
			return localTableEditor, nil
		} else if schema.SchemasAreEqual(tableSch, localTableEditor.tableEditor.Schema()) {
			return localTableEditor, nil
		}
		// Any existing references to this localTableEditor should be preserved, so we just change the underlying values
		localTableEditor.referencedTables = nil
		localTableEditor.referencingTables = nil
	} else {
		localTableEditor = &sessionedTableEditor{
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

	tableEditor, err := editor.NewTableEditor(ctx, t, tableSch, tableName, tes.opts)
	if err != nil {
		return nil, err
	}

	localTableEditor.tableEditor = tableEditor

	if tes.opts.ForeignKeyChecksDisabled {
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

// loadForeignKeys loads all tables mentioned in foreign keys for the given editor
func (tes *tableEditSession) loadForeignKeys(ctx context.Context, localTableEditor *sessionedTableEditor) error {
	// these are the tables that reference us, so we need to update them
	for _, foreignKey := range localTableEditor.referencingTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := tes.getTableEditor(ctx, foreignKey.TableName, nil)
		if err != nil {
			return err
		}
	}
	// these are the tables that we reference, so we need to refer to them
	for _, foreignKey := range localTableEditor.referencedTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := tes.getTableEditor(ctx, foreignKey.ReferencedTableName, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (tes *tableEditSession) setRoot(ctx context.Context, root *doltdb.RootValue) error {
	if root == nil {
		return fmt.Errorf("cannot set a tableEditSession's root to nil once it has been created")
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
			if err := localTableEditor.tableEditor.Close(ctx); err != nil {
				return err
			}
			delete(tes.tables, tableName)
			continue
		}
		tSch, err := t.GetSchema(ctx)
		if err != nil {
			return err
		}
		newTableEditor, err := editor.NewTableEditor(ctx, t, tSch, tableName, tes.opts)
		if err != nil {
			return err
		}
		if err := localTableEditor.tableEditor.Close(ctx); err != nil {
			return err
		}
		localTableEditor.tableEditor = newTableEditor
		localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
		if !tes.opts.ForeignKeyChecksDisabled {
			err = tes.loadForeignKeys(ctx, localTableEditor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
