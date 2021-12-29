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

package writer

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type writeSession struct {
	writers map[string]*sqlTableWriter
	root    *doltdb.RootValue
	mut     *sync.RWMutex

	opts editor.Options
}

var _ WriteSession = &writeSession{}

// CreatewriteSession creates and returns a writeSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func CreateWriteSession(root *doltdb.RootValue, opts editor.Options) WriteSession {
	return &writeSession{
		writers: make(map[string]*sqlTableWriter),
		root:    root,
		mut:     &sync.RWMutex{},
		opts:    opts,
	}
}

func (ws *writeSession) GetTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error) {
	ws.mut.Lock()
	defer ws.mut.Unlock()

	writer, ok := ws.writers[table]
	if ok {
		return writer, nil
	}

	var err error
	writer, err = ws.getTableWriter(ctx, table, database, ait, setter, batched)
	if err != nil {
		return nil, err
	}
	ws.writers[table] = writer

	return writer, nil
}

// Flush returns an updated root with all the changed writers.
func (ws *writeSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	ws.mut.Lock()
	defer ws.mut.Unlock()

	return ws.flush(ctx)
}

// UpdateRoot takes in a function meant to update the root (whether that be updating a table's schema, adding a foreign
// key, etc.) and passes in the flushed root. The function may then safely modify the root, and return the modified root
// (assuming no errors). The writeSession will update itself in accordance with the newly returned root.
func (ws *writeSession) UpdateRoot(ctx context.Context, cb func(ctx context.Context, current *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	ws.mut.Lock()
	defer ws.mut.Unlock()

	current, err := ws.flush(ctx)
	if err != nil {
		return err
	}

	mutated, err := cb(ctx, current)
	if err != nil {
		return err
	}

	return ws.setRoot(ctx, mutated)
}

func (ws *writeSession) GetOptions() editor.Options {
	return ws.opts
}

func (ws *writeSession) SetOptions(opts editor.Options) {
	ws.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (ws *writeSession) flush(ctx context.Context) (*doltdb.RootValue, error) {
	newRoot := ws.root
	rootLock := &sync.Mutex{}

	eg, ctx := errgroup.WithContext(ctx)
	for n := range ws.writers {
		// make local copies
		tableName, writer := n, ws.writers[n]

		eg.Go(func() error {
			t, err := writer.table(ctx)
			if err != nil {
				return err
			}

			rootLock.Lock()
			defer rootLock.Unlock()

			newRoot, err = newRoot.PutTable(ctx, tableName, t)
			return err
		})
	}

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	err = ws.setRoot(ctx, newRoot)
	if err != nil {
		return nil, nil
	}

	return ws.root, nil
}

// getTableWriter is the inner implementation for GetTableEditor, allowing recursive calls
func (ws *writeSession) getTableWriter(ctx context.Context, table, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (*sqlTableWriter, error) {
	tbl, ok, err := ws.root.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	tableEditor, err := editor.NewTableEditor(ctx, tbl, sch, table, ws.opts)
	if err != nil {
		return nil, err
	}

	conv := index.NewKVToSqlRowConverterForCols(tbl.Format(), sch)
	autoCol := autoIncrementColFromSchema(sch)

	writer := &sqlTableWriter{
		tableName:   table,
		dbName:      database,
		sch:         sch,
		autoIncCol:  autoCol,
		vrw:         ws.root.VRW(),
		kvToSQLRow:  conv,
		tableEditor: tableEditor,
		sess:        ws,
		batched:     batched,
		aiTracker:   ait,
		setter:      setter,
	}

	if ws.opts.ForeignKeyChecksDisabled {
		return writer, nil
	}

	writer.deps, err = ws.foreignKeysForWriter(ctx, table)
	if err != nil {
		return nil, err
	}

	return writer, nil
}

func (ws *writeSession) foreignKeysForWriter(ctx context.Context, tableName string) ([]writeDependency, error) {
	//// these are the writers that reference us, so we need to update them
	//for _, foreignKey := range localTableEditor.referencingTables {
	//	if !foreignKey.IsResolved() {
	//		continue
	//	}
	//	_, err := ws.getTableWriter(ctx, foreignKey.TableName, nil)
	//	if err != nil {
	//		return err
	//	}
	//}
	//// these are the writers that we reference, so we need to refer to them
	//for _, foreignKey := range localTableEditor.referencedTables {
	//	if !foreignKey.IsResolved() {
	//		continue
	//	}
	//	_, err := ws.getTableWriter(ctx, foreignKey.ReferencedTableName, nil)
	//	if err != nil {
	//		return err
	//	}
	//}
	//return nil
	return nil, nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (ws *writeSession) setRoot(ctx context.Context, root *doltdb.RootValue) error {
	if root == nil {
		return fmt.Errorf("cannot set a tableEditSession's root to nil once it has been created")
	}

	ws.root = root
	for name, writer := range ws.writers {
		tbl, ok, err := root.GetTable(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			// table was removed in newer root
			if err = writer.tableEditor.Close(ctx); err != nil {
				return err
			}
			delete(ws.writers, name)
			continue
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return err
		}

		err = writer.tableEditor.Close(ctx)
		if err != nil {
			return err
		}

		te, err := editor.NewTableEditor(ctx, tbl, sch, name, ws.opts)
		if err != nil {
			return err
		}
		writer.tableEditor = te

		writer.deps, err = ws.foreignKeysForWriter(ctx, name)
		if err != nil {
			return err
		}
	}

	return nil
}
