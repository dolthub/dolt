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
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetWorkingRoot before any
// table editors are returned.
func NewWriteSession(dbName string, ws *doltdb.WorkingSet, aiTracker *dsess.AutoIncrementTracker, setter dsess.SessionRootSetter, opts editor.Options) dsess.WriteSession {
	return &prollyWriteSession{
		dbName:        dbName,
		tables:        make(map[doltdb.TableName]*prollyTableWriter),
		aiTracker:     aiTracker,
		workingSet:    ws,
		setter:        setter,
		targetStaging: opts.TargetStaging,
	}
}

// prollyWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type prollyWriteSession struct {
	dbName        string
	tables        map[doltdb.TableName]*prollyTableWriter
	aiTracker     *dsess.AutoIncrementTracker
	workingSet    *doltdb.WorkingSet
	setter        dsess.SessionRootSetter
	targetStaging bool
	// deferMaterialization leaves the edits of completed statements in their table writers, rather than
	// materializing them into |workingSet| when each statement completes. The owner of the session must call
	// MaterializePending before reading |workingSet|.
	deferMaterialization bool
	// materializing is true while MaterializePending is running
	materializing bool
}

var _ dsess.WriteSession = &prollyWriteSession{}

func (s *prollyWriteSession) GetWorkingSet() *doltdb.WorkingSet {
	return s.workingSet
}

func (s *prollyWriteSession) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	for _, writer := range s.tables {
		err := writer.VisitGCRoots(ctx, roots)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetTableWriter implements WriteSession
func (s *prollyWriteSession) GetTableWriter(ctx *sql.Context, tblName doltdb.TableName) (dsess.TableWriter, error) {
	if tw, ok := s.tables[tblName]; ok {
		return tw, nil
	}

	// Certain table editors rely on this embedded working set. See
	// fullTextRewriteEditor for one example, where the |ctx| maintains
	// the old version of the data while fulltext indexes are rebuilt
	// using this hidden empty workingSet.
	var root doltdb.RootValue
	if s.targetStaging {
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
		dbName:    s.dbName,
		aiTracker: s.aiTracker,
		writeSess: s,
	}
	err = tblWriter.Reset(ctx, tbl)
	if err != nil {
		return nil, err
	}

	s.tables[tblName] = tblWriter
	return tblWriter, nil
}

// SetDeferMaterialization implements WriteSession
func (s *prollyWriteSession) SetDeferMaterialization(deferMaterialization bool) {
	s.deferMaterialization = deferMaterialization
}

// HasPendingEdits implements WriteSession
func (s *prollyWriteSession) HasPendingEdits() bool {
	if s.materializing {
		return false
	}
	for _, tblWriter := range s.tables {
		if tblWriter.pending {
			return true
		}
	}
	return false
}

// PendingTables implements WriteSession
func (s *prollyWriteSession) PendingTables() []doltdb.TableName {
	var names []doltdb.TableName
	for tblName, tblWriter := range s.tables {
		if tblWriter.pending {
			names = append(names, tblName)
		}
	}
	return names
}

// MaterializePending implements WriteSession
func (s *prollyWriteSession) MaterializePending(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	if s.materializing {
		return s.workingSet, nil
	}
	s.materializing = true
	defer func() {
		s.materializing = false
	}()

	var root doltdb.RootValue
	if s.targetStaging {
		root = s.workingSet.StagedRoot()
	} else {
		root = s.workingSet.WorkingRoot()
	}

	changed := false
	for tblName, tblWriter := range s.tables {
		if !tblWriter.pending {
			continue
		}
		tbl, err := tblWriter.materialize(ctx)
		if err != nil {
			return nil, err
		}
		root, err = root.PutTable(ctx, tblName, tbl)
		if err != nil {
			return nil, err
		}
		changed = true
	}

	if changed {
		if s.targetStaging {
			s.workingSet = s.workingSet.WithStagedRoot(root)
		} else {
			s.workingSet = s.workingSet.WithWorkingRoot(root)
		}
	}
	return s.workingSet, nil
}

// materializeAndSet materializes pending edits and sets the resulting root in the session.
func (s *prollyWriteSession) materializeAndSet(ctx *sql.Context) error {
	ws, err := s.MaterializePending(ctx)
	if err != nil {
		return err
	}
	if s.targetStaging {
		return s.setter(ctx, s.dbName, ws.StagedRoot())
	}
	return s.setter(ctx, s.dbName, ws.WorkingRoot())
}

// SetWorkingSet implements WriteSession
func (s *prollyWriteSession) SetWorkingSet(ctx *sql.Context, ws *doltdb.WorkingSet) error {
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
		var tbl *doltdb.Table
		var ok bool
		for tblName, tblWriter := range s.tables {
			tbl, ok, err = root.GetTable(ctx, tblName)
			if err != nil {
				return err
			}
			// table was removed in newer root
			if !ok {
				delete(s.tables, tblName)
				continue
			}
			err = tblWriter.Reset(ctx, tbl)
			if err != nil {
				return err
			}
		}
	}
	s.workingSet = ws
	return nil
}

// GetOptions implements WriteSession
func (s *prollyWriteSession) GetOptions() editor.Options {
	return editor.Options{
		TargetStaging: s.targetStaging,
	}
}

// SetOptions implements WriteSession
func (s *prollyWriteSession) SetOptions(opts editor.Options) {
	s.targetStaging = opts.TargetStaging
	return
}

// Flush implements WriteSessionFlusher
func (s *prollyWriteSession) Flush(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	_, err := s.flushAllTables(ctx)
	if err != nil {
		return nil, err
	}
	return s.workingSet, nil
}

// FlushTable puts the already materialized table into the working set
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
	err = s.setter(ctx, s.dbName, flushed)
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

// flushAllTables is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flushAllTables(ctx *sql.Context) (doltdb.RootValue, error) {
	type flushedTable struct {
		tblName doltdb.TableName
		tbl     *doltdb.Table
		err     error
	}

	// Flush each table and send results to the flushedTables channel
	flushedTables := make(chan flushedTable, 10)
	wg := sync.WaitGroup{}
	wg.Add(len(s.tables))
	for tblName, tblWriter := range s.tables {
		go func() {
			defer wg.Done()
			tbl, err := tblWriter.table(ctx)
			flushedTables <- flushedTable{
				tblName: tblName,
				tbl:     tbl,
				err:     err,
			}
		}()
	}

	var flushed doltdb.RootValue
	if s.targetStaging {
		flushed = s.workingSet.StagedRoot()
	} else {
		flushed = s.workingSet.WorkingRoot()
	}

	// Drain from the flushedTables channel and update RootValue with updated table
	eg := errgroup.Group{}
	eg.Go(func() error {
		var err error
		for flushedTbl := range flushedTables {
			if flushedTbl.err != nil {
				return flushedTbl.err
			}
			flushed, err = flushed.PutTable(ctx, flushedTbl.tblName, flushedTbl.tbl)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Wait for all tables to materialize before closing the flushedTables channel
	// Additionally, we must start the drain goroutine before blocking
	wg.Wait()
	close(flushedTables)

	// Wait for all flushed tables to be put onto the working set
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if s.targetStaging {
		s.workingSet = s.workingSet.WithStagedRoot(flushed)
	} else {
		s.workingSet = s.workingSet.WithWorkingRoot(flushed)
	}

	// TODO: seems like we should call s.setter to update the working set in the branch state,
	//  but doing so causes Binlog replication tests to fail?
	return flushed, nil
}
