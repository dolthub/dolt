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
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// prollyWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type prollyWriteSession struct {
	root *doltdb.RootValue

	tables map[string]*prollyWriter
	mut    *sync.RWMutex
}

var _ WriteSession = &prollyWriteSession{}

func (s *prollyWriteSession) GetTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	t, ok, err := s.root.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	pkSch, err := sqlutil.FromDoltSchema(table, sch)
	if err != nil {
		return nil, err
	}

	idx, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	m := durable.ProllyMapFromIndex(idx)
	mut := makeMutableProllyIndex(m, pkSch.Schema, sch)
	autoCol := autoIncrementColFromSchema(sch)

	return &prollyWriter{
		tableName:  table,
		dbName:     database,
		sch:        sch,
		mut:        mut,
		tbl:        t,
		autoIncCol: autoCol,
		aiTracker:  ait,
		sess:       s,
		setter:     setter,
		batched:    batched,
	}, nil
}

// Flush returns an updated root with all of the changed tables.
func (s *prollyWriteSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.flush(ctx)
}

// SetRoot uses the given root to set all open table editors to the state as represented in the root. If any
// tables are removed in the root, but have open table editors, then the references to those are removed. If those
// removed table's editors are used after this, then the behavior is undefined. This will lose any changes that have not
// been flushed. If the purpose is to add a new table, foreign key, etc. (using Flush followed up with SetRoot), then
// use UpdateRoot. Calling the two functions manually for the purposes of root modification may lead to race conditions.
func (s *prollyWriteSession) SetRoot(ctx context.Context, root *doltdb.RootValue) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.setRoot(ctx, root)
}

// UpdateRoot takes in a function meant to update the root (whether that be updating a table's schema, adding a foreign
// key, etc.) and passes in the flushed root. The function may then safely modify the root, and return the modified root
// (assuming no errors). The prollyWriteSession will update itself in accordance with the newly returned root.
func (s *prollyWriteSession) UpdateRoot(ctx context.Context, cb func(ctx context.Context, current *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	current, err := s.flush(ctx)
	if err != nil {
		return err
	}

	mutated, err := cb(ctx, current)
	if err != nil {
		return err
	}

	return s.setRoot(ctx, mutated)
}

func (s *prollyWriteSession) GetOptions() editor.Options {
	return editor.Options{}
}

func (s *prollyWriteSession) SetOptions(opts editor.Options) {
	return
}

// flush is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flush(ctx context.Context) (*doltdb.RootValue, error) {

	var err error
	tables := make(map[string]*doltdb.Table, len(s.tables))
	mu := &sync.Mutex{}

	eg, ctx := errgroup.WithContext(ctx)
	for n := range s.tables {
		name := n // make a copy
		eg.Go(func() error {
			wr := s.tables[name]
			t, err := wr.table(ctx)
			if err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()
			tables[name] = t
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	for name, tbl := range tables {
		s.root, err = s.root.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}

	return s.root, nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (s *prollyWriteSession) setRoot(ctx context.Context, root *doltdb.RootValue) error {
	for name := range s.tables {
		delete(s.tables, name)
	}
	s.root = root
	return nil
}
