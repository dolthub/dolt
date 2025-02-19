// Copyright 2025 Dolthub, Inc.
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

package sqle

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

// Auto GC is the ability of a running SQL server engine to perform
// dolt_gc() behaviors periodically. If enabled, it currently works as
// follows:
//
// An AutoGCController is created for a running SQL Engine. The
// controller runs a background thread which is only ever running one
// GC at a time. Post Commit Hooks are installed on every database in
// the DoltDatabaseProvider for the SQL Engine. Those hooks check if
// it is time to perform a GC for that particular database. If it is,
// they forward a request the background thread to register the
// database as wanting a GC.

type AutoGCController struct {
	workCh chan autoGCWork
	lgr    *logrus.Logger
}

func NewAutoGCController(lgr *logrus.Logger) *AutoGCController {
	return &AutoGCController{
		workCh: make(chan autoGCWork),
		lgr:    lgr,
	}
}

// Passed by a commit hook to the auto-GC thread, requesting the
// thread to dolt_gc |db|. When the GC is finished, |done| will be
// closed. Signalling completion allows the commit hook to only
// submit one dolt_gc request at a time.
type autoGCWork struct {
	db   *doltdb.DoltDB
	done chan struct{}
	name string // only for logging.
}

// During engine initialization, this should be called to ensure the
// background worker threads responsible for performing the GC are
// running.
func (c *AutoGCController) RunBackgroundThread(threads *sql.BackgroundThreads, ctxF func(context.Context) (*sql.Context, error)) error {
	return threads.Add("auto_gc_thread", func(ctx context.Context) {
		var wg sync.WaitGroup
		runCh := make(chan autoGCWork)
		wg.Add(1)
		go func() {
			defer wg.Done()
			dbs := make([]autoGCWork, 0)
			// Accumulate GC requests, only one will come in per database at a time.
			// Send the oldest one out to the worker when it is ready.
			for {
				var toSendCh chan autoGCWork
				var toSend autoGCWork
				if len(dbs) > 0 {
					toSend = dbs[0]
					toSendCh = runCh
				}
				select {
				case <-ctx.Done():
					// sql.BackgroundThreads is shutting down.
					// No need to drain or anything; just
					// return.
					return
				case newDB := <-c.workCh:
					dbs = append(dbs, newDB)
				case toSendCh <- toSend:
					// We just sent the front of the slice.
					// Delete it from our set of pending GCs.
					copy(dbs[:], dbs[1:])
					dbs = dbs[:len(dbs)-1]
				}

			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case work := <-runCh:
					c.doWork(ctx, work, ctxF)
				}
			}
		}()
		wg.Wait()
	})
}

func (c *AutoGCController) doWork(ctx context.Context, work autoGCWork, ctxF func(context.Context) (*sql.Context, error)) {
	defer close(work.done)
	sqlCtx, err := ctxF(ctx)
	if err != nil {
		c.lgr.Warnf("sqle/auto_gc: Could not create session to GC %s: %v", work.name, err)
		return
	}
	c.lgr.Tracef("sqle/auto_gc: Beginning auto GC of database %s", work.name)
	start := time.Now()
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	err = dprocedures.RunDoltGC(sqlCtx, work.db, types.GCModeDefault, work.name)
	if err != nil {
		c.lgr.Warnf("sqle/auto_gc: Attempt to auto GC database %s failed with error: %v", work.name, err)
		return
	}
	c.lgr.Infof("sqle/auto_gc: Successfully completed auto GC of database %s in %v", work.name, time.Since(start))
}

func (c *AutoGCController) newCommitHook(name string) doltdb.CommitHook {
	closed := make(chan struct{})
	close(closed)
	return &autoGCCommitHook{
		c:    c,
		name: name,
		done: closed,
		next: make(chan struct{}),
	}
}

// The doltdb.CommitHook which watches for database changes and
// requests dolt_gcs.
type autoGCCommitHook struct {
	c    *AutoGCController
	name string
	// When |done| is closed, there is no GC currently running or
	// pending for this database. If it is open, then there is a
	// pending request for GC or a GC is currently running. Once
	// |done| is closed, we can check for auto GC conditions on
	// the database to see if we should request a new GC.
	done chan struct{}
	// It simplifies the logic and efficiency of the
	// implementation a bit to have an already allocated channel
	// we can try to send when we request a GC. If will become our
	// new |done| channel once we send it successfully.
	next chan struct{}
	// lastSz is set the first time we observe StoreSizes after a
	// GC or after the server comes up. It is used in some simple
	// growth heuristics to figure out if we want to run a GC. We
	// set it back to |nil| when we successfully submit a request
	// to GC, so that we observe and store the new size after the
	// GC is finished.
	lastSz *doltdb.StoreSizes
	// |done|, |next|, |lastSz| are mutable and |Execute| can be
	// called concurrently. We protect them with |mu|.
	mu sync.Mutex
}

// During engine initialization, called on the original set of
// databases to configure them for auto-GC.
func (c *AutoGCController) ApplyCommitHooks(ctx context.Context, mrEnv *env.MultiRepoEnv, dbs ...dsess.SqlDatabase) error {
	for _, db := range dbs {
		denv := mrEnv.GetEnv(db.Name())
		if denv == nil {
			continue
		}
		denv.DoltDB(ctx).PrependCommitHooks(ctx, c.newCommitHook(db.Name()))
	}
	return nil
}

func (c *AutoGCController) InitDatabaseHook() InitDatabaseHook {
	return func(ctx *sql.Context, pro *DoltDatabaseProvider, name string, env *env.DoltEnv, db dsess.SqlDatabase) error {
		env.DoltDB(ctx).PrependCommitHooks(ctx, c.newCommitHook(name))
		return nil
	}
}

func (h *autoGCCommitHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-h.done:
		sz, err := db.StoreSizes(ctx)
		if err != nil {
			// Something is probably quite wrong. Regardless, can't determine if we should GC.
			return nil, err
		}
		if h.lastSz == nil {
			h.lastSz = &sz
		}
		const size_128mb = (1 << 27)
		const size_256mb = (1 << 28)
		if sz.JournalBytes > size_128mb {
			// Our first heuristic is simply if journal is greater than a fixed size...
			return nil, h.requestGC(ctx, db)
		} else if sz.TotalBytes > h.lastSz.TotalBytes && sz.TotalBytes - h.lastSz.TotalBytes > size_256mb {
			// Or if the store has grown by a fixed size since our last GC / we started watching it...
			return nil, h.requestGC(ctx, db)
		}
	default:
		// A GC is already running or pending. No need to check.
	}
	return nil, nil
}

func (h *autoGCCommitHook) requestGC(ctx context.Context, db *doltdb.DoltDB) error {
	select {
	case h.c.workCh <- autoGCWork{db, h.next, h.name}:
		h.done = h.next
		h.next = make(chan struct{})
		h.lastSz = nil
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *autoGCCommitHook) HandleError(ctx context.Context, err error) error {
	return nil
}

func (h *autoGCCommitHook) SetLogger(ctx context.Context, wr io.Writer) error {
	return nil
}

func (h *autoGCCommitHook) ExecuteForWorkingSets() bool {
	return true
}
