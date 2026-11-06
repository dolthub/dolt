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

	"github.com/dolthub/go-mysql-server/errguard"
	"github.com/dolthub/go-mysql-server/sql"

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

// HasTableWriters implements WriteSession
func (s *prollyWriteSession) HasTableWriters() bool {
	return len(s.tables) > 0
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
	err = s.publish(ctx)
	if err != nil {
		return nil, err
	}
	return flushed, nil
}

// publish hands this write session's root to the session. A write session can start ahead of the session, as a
// table rewrite's does, and its root reaches the session only through here.
func (s *prollyWriteSession) publish(ctx *sql.Context) error {
	if s.targetStaging {
		return s.setter(ctx, s.dbName, s.workingSet.StagedRoot())
	}
	return s.setter(ctx, s.dbName, s.workingSet.WorkingRoot())
}

// flushAllTables is the inner implementation for Flush that does not acquire any locks
func (s *prollyWriteSession) flushAllTables(ctx *sql.Context) (doltdb.RootValue, error) {
	type flushedTable struct {
		tblName doltdb.TableName
		tbl     *doltdb.Table
	}

	// Materialize each table concurrently.
	flushedTables := make([]flushedTable, len(s.tables))
	eg, egCtx := ctx.NewErrgroup()
	i := 0
	for tblName, tblWriter := range s.tables {
		res := &flushedTables[i]
		i++
		errguard.Go(eg, func() error {
			tbl, err := tblWriter.table(egCtx)
			if err != nil {
				return err
			}
			*res = flushedTable{tblName: tblName, tbl: tbl}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var flushed doltdb.RootValue
	if s.targetStaging {
		flushed = s.workingSet.StagedRoot()
	} else {
		flushed = s.workingSet.WorkingRoot()
	}
	for _, ft := range flushedTables {
		var err error
		flushed, err = flushed.PutTable(ctx, ft.tblName, ft.tbl)
		if err != nil {
			return nil, err
		}
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
