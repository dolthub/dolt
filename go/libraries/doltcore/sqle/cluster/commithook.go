// Copyright 2022 Dolthub, Inc.
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

package cluster

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ doltdb.CommitHook = (*commithook)(nil)

type commithook struct {
	lgr               *logrus.Entry
	remotename        string
	dbname            string
	lout              io.Writer
	mu                sync.Mutex
	wg                sync.WaitGroup
	cond              *sync.Cond
	nextHead          hash.Hash
	lastPushedHead    hash.Hash
	lastPushedSuccess time.Time
	nextPushAttempt   time.Time

	role Role

	// The standby replica to which the new root gets replicated.
	destDB *doltdb.DoltDB
	// When we first start replicating to the destination, we lazily
	// instantiate the remote and we do not treat failures as terminal.
	destDBF func(context.Context) (*doltdb.DoltDB, error)
	// This database, which we are replicating from. In our current
	// configuration, it is local to this server process.
	srcDB *doltdb.DoltDB

	tempDir string
}

var errDestDBRootHashMoved error = errors.New("sqle: cluster: standby replication: destination database root hash moved during our write, while it is assumed we are the only writer.")

func newCommitHook(lgr *logrus.Logger, remotename, dbname string, role Role, destDBF func(context.Context) (*doltdb.DoltDB, error), srcDB *doltdb.DoltDB, tempDir string) *commithook {
	var ret commithook
	ret.lgr = lgr.WithField("thread", "Standby Replication - "+dbname+" to "+remotename)
	ret.remotename = remotename
	ret.dbname = dbname
	ret.role = role
	ret.destDBF = destDBF
	ret.srcDB = srcDB
	ret.tempDir = tempDir
	ret.cond = sync.NewCond(&ret.mu)
	return &ret
}

func (h *commithook) Run(bt *sql.BackgroundThreads) error {
	return bt.Add("Standby Replication - "+h.dbname+" to "+h.remotename, h.run)
}

func (h *commithook) replicate(ctx context.Context) {
	defer h.wg.Done()
	defer h.lgr.Tracef("cluster/commithook: background thread: replicate: shutdown.")
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		// Shutdown for context canceled.
		if ctx.Err() != nil {
			h.lgr.Tracef("commithook replicate thread exiting; saw ctx.Err(): %v", ctx.Err())
			if h.shouldReplicate() {
				// attempt a last true-up of our standby as we shutdown
				// TODO: context.WithDeadline based on config / convention?
				h.attemptReplicate(context.Background())
			}
			return
		}
		if h.role == RolePrimary && h.nextHead == (hash.Hash{}) {
			h.lgr.Tracef("cluster/commithook: fetching current head.")
			// When the replicate thread comes up, it attempts to replicate the current head.
			datasDB := doltdb.HackDatasDatabaseFromDoltDB(h.srcDB)
			cs := datas.ChunkStoreFromDatabase(datasDB)
			var err error
			h.nextHead, err = cs.Root(ctx)
			if err != nil {
				// TODO: if err != nil, something is really wrong; should shutdown or backoff.
				h.lgr.Warning("standby replication thread failed to load database root: %v", err)
				h.nextHead = hash.Hash{}
			}
		} else if h.shouldReplicate() {
			h.attemptReplicate(ctx)
		} else {
			h.lgr.Tracef("cluster/commithook: background thread: waiting for signal.")
			h.cond.Wait()
			h.lgr.Tracef("cluster/commithook: background thread: woken up.")
		}
	}
}

// called with h.mu locked.
func (h *commithook) shouldReplicate() bool {
	if h.role != RolePrimary {
		return false
	}
	if h.nextHead == h.lastPushedHead {
		return false
	}
	return (h.nextPushAttempt == (time.Time{}) || time.Now().After(h.nextPushAttempt))
}

// Called by the replicate thread to push the nextHead to the destDB and set
// its root to the new value.
//
// preconditions: h.mu is locked and shouldReplicate() returned true.
// when this function returns, h.mu is locked.
func (h *commithook) attemptReplicate(ctx context.Context) {
	toPush := h.nextHead
	destDB := h.destDB
	h.mu.Unlock()

	if destDB == nil {
		h.lgr.Tracef("cluster/commithook: attempting to fetch destDB.")
		var err error
		destDB, err = h.destDBF(ctx)
		if err != nil {
			h.lgr.Warnf("cluster/commithook: could not replicate to standby: error fetching destDB: %v.", err)
			h.mu.Lock()
			// TODO: We could add some backoff here.
			h.nextPushAttempt = time.Now().Add(1 * time.Second)
			return
		}
		h.lgr.Tracef("cluster/commithook: fetched destDB")
		h.mu.Lock()
		h.destDB = destDB
		h.mu.Unlock()
	}

	h.lgr.Tracef("cluster/commithook: pushing chunks for root hash %v to destDB", toPush.String())
	err := destDB.PullChunks(ctx, h.tempDir, h.srcDB, toPush, nil, nil)
	if err == nil {
		h.lgr.Tracef("cluster/commithook: successfully pushed chunks, setting root")
		datasDB := doltdb.HackDatasDatabaseFromDoltDB(destDB)
		cs := datas.ChunkStoreFromDatabase(datasDB)
		var curRootHash hash.Hash
		if curRootHash, err = cs.Root(ctx); err == nil {
			var ok bool
			ok, err = cs.Commit(ctx, toPush, curRootHash)
			if err == nil && !ok {
				err = errDestDBRootHashMoved
			}
		}
	}

	h.mu.Lock()
	if err == nil {
		h.lgr.Tracef("cluster/commithook: successfully Commited chunks on destDB")
		h.lastPushedHead = toPush
		h.lastPushedSuccess = time.Now()
		h.nextPushAttempt = time.Time{}
	} else {
		h.lgr.Warnf("cluster/commithook: failed to commit chunks on destDB: %v", err)
		// add some delay if a new head didn't come in while we were pushing.
		if toPush == h.nextHead {
			// TODO: We could add some backoff here.
			h.nextPushAttempt = time.Now().Add(1 * time.Second)
		}
	}
}

// TODO: Would be more efficient to only tick when we have outstanding work...
func (h *commithook) tick(ctx context.Context) {
	defer h.wg.Done()
	defer h.lgr.Trace("tick thread returning")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			time.Sleep(1 * time.Second)
			return
		case <-ticker.C:
			h.cond.Signal()
		}
	}
}

func (h *commithook) run(ctx context.Context) {
	// The hook comes up attempting to replicate the current head.
	h.lgr.Tracef("cluster/commithook: background thread: running.")
	h.wg.Add(2)
	go h.replicate(ctx)
	go h.tick(ctx)
	<-ctx.Done()
	h.lgr.Tracef("cluster/commithook: background thread: requested shutdown, signaling replication thread.")
	h.cond.Signal()
	h.wg.Wait()
	h.lgr.Tracef("cluster/commithook: background thread: completed.")
}

func (h *commithook) setRole(role Role) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Reset head-to-push and timers here. When we transition into Primary,
	// the replicate() loop will take these from the current chunk store.
	h.nextHead = hash.Hash{}
	h.lastPushedHead = hash.Hash{}
	h.lastPushedSuccess = time.Time{}
	h.nextPushAttempt = time.Time{}
	h.role = role
	h.cond.Signal()
}

// Execute on this commithook updates the target root hash we're attempting to
// replicate and wakes the replication thread.
func (h *commithook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	cs := datas.ChunkStoreFromDatabase(db)
	root, err := cs.Root(ctx)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if root != h.nextHead {
		h.nextHead = root
		h.nextPushAttempt = time.Time{}
		h.cond.Signal()
	}
	return nil
}

func (h *commithook) HandleError(ctx context.Context, err error) error {
	return nil
}

func (h *commithook) SetLogger(ctx context.Context, wr io.Writer) error {
	h.lout = wr
	return nil
}
