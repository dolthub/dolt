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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ doltdb.CommitHook = (*commithook)(nil)

type commithook struct {
	rootLgr              *logrus.Entry
	lgr                  atomic.Value // *logrus.Entry
	remotename           string
	dbname               string
	mu                   sync.Mutex
	wg                   sync.WaitGroup
	cond                 *sync.Cond
	nextHead             hash.Hash
	lastPushedHead       hash.Hash
	nextPushAttempt      time.Time
	nextHeadIncomingTime time.Time
	lastSuccess          time.Time
	currentError         *string
	cancelReplicate      func()

	// waitNotify is set by controller when it needs to track whether the
	// commithooks are caught up with replicating to the standby.
	waitNotify func()

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

var errDestDBRootHashMoved error = errors.New("cluster/commithook: standby replication: destination database root hash moved during our write, while it is assumed we are the only writer.")

const logFieldThread = "thread"
const logFieldRole = "role"

func newCommitHook(lgr *logrus.Logger, remotename, dbname string, role Role, destDBF func(context.Context) (*doltdb.DoltDB, error), srcDB *doltdb.DoltDB, tempDir string) *commithook {
	var ret commithook
	ret.rootLgr = lgr.WithField(logFieldThread, "Standby Replication - "+dbname+" to "+remotename)
	ret.lgr.Store(ret.rootLgr.WithField(logFieldRole, string(role)))
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

func (h *commithook) run(ctx context.Context) {
	// The hook comes up attempting to replicate the current head.
	h.logger().Tracef("cluster/commithook: background thread: running.")
	h.wg.Add(2)
	go h.replicate(ctx)
	go h.tick(ctx)
	<-ctx.Done()
	h.logger().Tracef("cluster/commithook: background thread: requested shutdown, signaling replication thread.")
	h.cond.Signal()
	h.wg.Wait()
	h.logger().Tracef("cluster/commithook: background thread: completed.")
}

func (h *commithook) replicate(ctx context.Context) {
	defer h.wg.Done()
	defer h.logger().Tracef("cluster/commithook: background thread: replicate: shutdown.")
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		lgr := h.logger()
		// Shutdown for context canceled.
		if ctx.Err() != nil {
			lgr.Tracef("cluster/commithook replicate thread exiting; saw ctx.Err(): %v", ctx.Err())
			if h.shouldReplicate() {
				// attempt a last true-up of our standby as we shutdown
				// TODO: context.WithDeadline based on config / convention?
				h.attemptReplicate(context.Background())
			}
			return
		}
		if h.primaryNeedsInit() {
			lgr.Tracef("cluster/commithook: fetching current head.")
			// When the replicate thread comes up, it attempts to replicate the current head.
			datasDB := doltdb.HackDatasDatabaseFromDoltDB(h.srcDB)
			cs := datas.ChunkStoreFromDatabase(datasDB)
			var err error
			h.nextHead, err = cs.Root(ctx)
			if err != nil {
				// TODO: if err != nil, something is really wrong; should shutdown or backoff.
				lgr.Warningf("standby replication thread failed to load database root: %v", err)
				h.nextHead = hash.Hash{}
			}

			// We do not know when this head was written, but we
			// are starting to try to replicate it now.
			h.nextHeadIncomingTime = time.Now()
		} else if h.shouldReplicate() {
			h.attemptReplicate(ctx)
		} else {
			lgr.Tracef("cluster/commithook: background thread: waiting for signal.")
			if h.waitNotify != nil {
				h.waitNotify()
			}
			h.cond.Wait()
			lgr.Tracef("cluster/commithook: background thread: woken up.")
		}
	}
}

// called with h.mu locked.
func (h *commithook) shouldReplicate() bool {
	if h.isCaughtUp() {
		return false
	}
	return (h.nextPushAttempt == (time.Time{}) || time.Now().After(h.nextPushAttempt))
}

// called with h.mu locked. Returns true if the standby is true-d up, false
// otherwise. Different from shouldReplicate() in that it does not care about
// nextPushAttempt, for example. Used in Controller.waitForReplicate.
func (h *commithook) isCaughtUp() bool {
	if h.role != RolePrimary {
		return true
	}
	return h.nextHead == h.lastPushedHead
}

func (h *commithook) isCaughtUpLocking() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.isCaughtUp()
}

// called with h.mu locked.
func (h *commithook) primaryNeedsInit() bool {
	return h.role == RolePrimary && h.nextHead == (hash.Hash{})
}

// Called by the replicate thread to push the nextHead to the destDB and set
// its root to the new value.
//
// preconditions: h.mu is locked and shouldReplicate() returned true.
// when this function returns, h.mu is locked.
func (h *commithook) attemptReplicate(ctx context.Context) {
	lgr := h.logger()
	toPush := h.nextHead
	incomingTime := h.nextHeadIncomingTime
	destDB := h.destDB
	ctx, h.cancelReplicate = context.WithCancel(ctx)
	defer func() {
		if h.cancelReplicate != nil {
			h.cancelReplicate()
		}
		h.cancelReplicate = nil
	}()
	h.mu.Unlock()

	if destDB == nil {
		lgr.Tracef("cluster/commithook: attempting to fetch destDB.")
		var err error
		destDB, err = h.destDBF(ctx)
		if err != nil {
			h.currentError = new(string)
			*h.currentError = fmt.Sprintf("could not replicate to standby: error fetching destDB: %v", err)
			lgr.Warnf("cluster/commithook: could not replicate to standby: error fetching destDB: %v.", err)
			h.mu.Lock()
			// TODO: We could add some backoff here.
			if toPush == h.nextHead {
				h.nextPushAttempt = time.Now().Add(1 * time.Second)
			}
			h.cancelReplicate = nil
			return
		}
		lgr.Tracef("cluster/commithook: fetched destDB")
		h.mu.Lock()
		h.destDB = destDB
		h.mu.Unlock()
	}

	lgr.Tracef("cluster/commithook: pushing chunks for root hash %v to destDB", toPush.String())
	err := destDB.PullChunks(ctx, h.tempDir, h.srcDB, []hash.Hash{toPush}, nil, nil)
	if err == nil {
		lgr.Tracef("cluster/commithook: successfully pushed chunks, setting root")
		datasDB := doltdb.HackDatasDatabaseFromDoltDB(destDB)
		cs := datas.ChunkStoreFromDatabase(datasDB)
		var curRootHash hash.Hash
		if err = cs.Rebase(ctx); err == nil {
			if curRootHash, err = cs.Root(ctx); err == nil {
				var ok bool
				ok, err = cs.Commit(ctx, toPush, curRootHash)
				if err == nil && !ok {
					err = errDestDBRootHashMoved
				}
			}
		}
	}

	h.mu.Lock()
	if h.role == RolePrimary {
		if err == nil {
			h.currentError = nil
			lgr.Tracef("cluster/commithook: successfully Commited chunks on destDB")
			h.lastPushedHead = toPush
			h.lastSuccess = incomingTime
			h.nextPushAttempt = time.Time{}
		} else {
			h.currentError = new(string)
			*h.currentError = fmt.Sprintf("failed to commit chunks on destDB: %v", err)
			lgr.Warnf("cluster/commithook: failed to commit chunks on destDB: %v", err)
			// add some delay if a new head didn't come in while we were pushing.
			if toPush == h.nextHead {
				// TODO: We could add some backoff here.
				h.nextPushAttempt = time.Now().Add(1 * time.Second)
			}
		}
	}
}

func (h *commithook) status() (replicationLag *time.Duration, lastUpdate *time.Time, currentErr *string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.role == RolePrimary {
		if h.lastPushedHead != (hash.Hash{}) {
			replicationLag = new(time.Duration)
			if h.nextHead != h.lastPushedHead {
				// We return the wallclock time between now and the last time we were
				// successful. If h.nextHeadIncomingTime is significantly earlier than
				// time.Now(), because the server has not received a write in a long
				// time, then this metric may report a high number when the number of
				// seconds of writes outstanding could actually be much smaller.
				// Operationally, failure to replicate a write for a long time is a
				// problem that merits investigation, regardless of how many pending
				// writes are failing to replicate.
				*replicationLag = time.Now().Sub(h.lastSuccess)
			}
		}

	}

	if h.lastSuccess != (time.Time{}) {
		lastUpdate = new(time.Time)
		*lastUpdate = h.lastSuccess
	}

	currentErr = h.currentError

	return
}

func (h *commithook) logger() *logrus.Entry {
	return h.lgr.Load().(*logrus.Entry)
}

// TODO: Would be more efficient to only tick when we have outstanding work...
func (h *commithook) tick(ctx context.Context) {
	defer h.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.cond.Signal()
		}
	}
}

func (h *commithook) recordSuccessfulRemoteSrvCommit() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.role != RoleStandby {
		return
	}
	h.lastSuccess = time.Now()
	h.currentError = nil
}

func (h *commithook) setRole(role Role) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Reset head-to-push and timers here. When we transition into Primary,
	// the replicate() loop will take these from the current chunk store.
	h.currentError = nil
	h.nextHead = hash.Hash{}
	h.lastPushedHead = hash.Hash{}
	h.lastSuccess = time.Time{}
	h.nextPushAttempt = time.Time{}
	h.role = role
	h.lgr.Store(h.rootLgr.WithField(logFieldRole, string(role)))
	if h.cancelReplicate != nil {
		h.cancelReplicate()
		h.cancelReplicate = nil
	}
	if role == RoleDetectedBrokenConfig {
		h.currentError = &errDetectedBrokenConfigStr
	}
	h.cond.Signal()
}

func (h *commithook) setWaitNotify(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.waitNotify = f
}

var errDetectedBrokenConfigStr = "error: more than one server was configured as primary in the same epoch. this server has stopped accepting writes. choose a primary in the cluster and call dolt_assume_cluster_role() on servers in the cluster to start replication at a higher epoch"

// Execute on this commithook updates the target root hash we're attempting to
// replicate and wakes the replication thread.
func (h *commithook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	lgr := h.logger()
	lgr.Warnf("cluster/commithook: Execute called post commit")
	cs := datas.ChunkStoreFromDatabase(db)
	root, err := cs.Root(ctx)
	if err != nil {
		lgr.Warnf("cluster/commithook: Execute: error retrieving local database root: %v", err)
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	lgr = h.logger()
	if h.role != RolePrimary {
		lgr.Warnf("cluster/commithook received commit callback for a commit on %s, but we are not role primary; not replicating the commit, which is likely to be lost.", ds.ID())
		return nil
	}
	if root != h.nextHead {
		lgr.Tracef("signaling replication thread to push new head: %v", root.String())
		h.nextHeadIncomingTime = time.Now()
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
	return nil
}

func (h *commithook) ExecuteForWorkingSets() bool {
	return true
}
