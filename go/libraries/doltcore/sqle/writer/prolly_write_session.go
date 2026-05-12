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

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetWorkingRoot before any
// table editors are returned.
func NewWriteSession(ws *doltdb.WorkingSet, aiTracker globalstate.AutoIncrementTracker, opts editor.Options) dsess.WriteSession {
	return &prollyWriteSession{
		workingSet:    ws,
		tables:        make(map[doltdb.TableName]*prollyTableWriter),
		aiTracker:     aiTracker,
		mut:           &sync.RWMutex{},
		targetStaging: opts.TargetStaging,
	}
}

// prollyWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type prollyWriteSession struct {
	workingSet    *doltdb.WorkingSet
	tables        map[doltdb.TableName]*prollyTableWriter
	aiTracker     globalstate.AutoIncrementTracker
	mut           *sync.RWMutex
	targetStaging bool
}

var _ dsess.WriteSession = &prollyWriteSession{}

func (s *prollyWriteSession) GetWorkingSet() *doltdb.WorkingSet {
	return s.workingSet
}

func (s *prollyWriteSession) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, writer := range s.tables {
		err := writer.VisitGCRoots(ctx, roots)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetTableWriter implemented WriteSession.
func (s *prollyWriteSession) GetTableWriter(ctx *sql.Context, tblName doltdb.TableName, db string, setter dsess.SessionRootSetter, targetStaging bool) (dsess.TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if tw, ok := s.tables[tblName]; ok {
		return tw, nil
	}

	// Certain table editors rely on this embedded working set. See
	// fullTextRewriteEditor for one example, where the |ctx| maintains
	// the old version of the data while fulltext indexes are rebuilt
	// using this hidden empty workingSet.
	var root doltdb.RootValue
	if targetStaging {
		root = s.workingSet.StagedRoot()
	} else {
		root = s.workingSet.WorkingRoot()
	}

	tbl, ok, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	tblWriter := &prollyTableWriter{
		tblName:   tblName,
		dbName:    db,
		aiTracker: s.aiTracker,
		writeSess: s,
		setter:    setter,
	}
	err = tblWriter.Reset(ctx, tbl)
	if err != nil {
		return nil, err
	}

	s.tables[tblName] = tblWriter
	return tblWriter, nil
}

// Flush implemented WriteSession.
func (s *prollyWriteSession) Flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	_, err := s.flushAllTables(ctx, false, nil)
	if err != nil {
		return nil, err
	}
	return s.workingSet, nil
}

func (s *prollyWriteSession) FlushWithAutoIncrementOverrides(ctx *sql.Context, autoIncSet bool, autoIncrements map[string]uint64) (doltdb.RootValue, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flushAllTables(ctx, autoIncSet, autoIncrements)
}

// SetWorkingSet implements WriteSession.
func (s *prollyWriteSession) SetWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.setWorkingSet(ctx, ws)
}

// GetOptions implemented WriteSession.
func (s *prollyWriteSession) GetOptions() editor.Options {
	return editor.Options{}
}

// SetOptions implemented WriteSession.
func (s *prollyWriteSession) SetOptions(opts editor.Options) {
	return
}

func (s *prollyWriteSession) materializeTable(ctx *sql.Context, tblName doltdb.TableName, tblWriter *prollyTableWriter, autoIncSet, manualAutoInc bool, manualAutoIncVal uint64) (*doltdb.Table, error) {
	// Materialize table
	tbl, err := tblWriter.table(ctx)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (s *prollyWriteSession) FlushTable(ctx *sql.Context, tblName doltdb.TableName, tbl *doltdb.Table) (flushed doltdb.RootValue, err error) {
	if s.targetStaging {
		flushed = s.workingSet.StagedRoot()
		flushed, err = flushed.PutTable(ctx, tblName, tbl)
		if err != nil {
			return nil, err
		}
		s.workingSet = s.workingSet.WithStagedRoot(flushed)
	} else {
		flushed = s.workingSet.WorkingRoot()
		flushed, err = flushed.PutTable(ctx, tblName, tbl)
		if err != nil {
			return nil, err
		}
		s.workingSet = s.workingSet.WithWorkingRoot(flushed)
	}

	return flushed, nil
}

// flushAllTables is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flushAllTables(ctx *sql.Context, autoIncSet bool, manualAutoIncrementsSettings map[string]uint64) (doltdb.RootValue, error) {
	type result struct {
		tblName doltdb.TableName
		tbl     *doltdb.Table
		err     error
	}

	// Start flushing each table, and send to results channel
	results := make(chan result, 10)
	wg := sync.WaitGroup{}
	wg.Add(len(s.tables))
	for tblName, tblWriter := range s.tables {
		go func() {
			defer wg.Done()
			//manualAutoIncVal, manualAutoInc := manualAutoIncrementsSettings[tblName.Name]
			//tbl, err := s.materializeTable(ctx, tblName, autoIncSet, manualAutoInc, manualAutoIncVal)
			tbl, err := tblWriter.table(ctx)
			results <- result{
				tblName: tblName,
				tbl:     tbl,
				err:     err,
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	// Drain from results channel, updating RootValue each time
	var flushed doltdb.RootValue
	if s.targetStaging {
		flushed = s.workingSet.StagedRoot()
	} else {
		flushed = s.workingSet.WorkingRoot()
	}
	eg := errgroup.Group{}
	eg.Go(func() error {
		for res := range results {
			if res.err != nil {
				return res.err
			}
			var err error
			flushed, err = flushed.PutTable(ctx, res.tblName, res.tbl)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if s.targetStaging {
		s.workingSet = s.workingSet.WithStagedRoot(flushed)
	} else {
		s.workingSet = s.workingSet.WithWorkingRoot(flushed)
	}
	return flushed, nil
}

// setRoot is the inner implementation for SetWorkingRoot that does not acquire any locks (it's only called from a function that acquires locks???)
func (s *prollyWriteSession) setWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
	root := ws.WorkingRoot()
	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}
	workingRootHash, err := s.workingSet.WorkingRoot().HashOf()
	if err != nil {
		return err
	}
	if rootHash != workingRootHash {
		for tableName, tableWriter := range s.tables {
			tbl, ok, err := root.GetTable(ctx, tableName)
			if err != nil {
				return err
			}

			// table was removed in newer root
			if !ok {
				delete(s.tables, tableName)
				continue
			}

			err = tableWriter.Reset(ctx, tbl)
			if err != nil {
				return err
			}
		}
	}
	s.workingSet = ws
	return nil
}
