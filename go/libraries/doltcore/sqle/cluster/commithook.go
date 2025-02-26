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
var _ doltdb.NotifyWaitFailedCommitHook = (*commithook)(nil)

type commithook struct {
	rootLgr              *logrus.Entry
	lgr                  atomic.Value // *logrus.Entry
	remotename           string
	remoteurl            string
	dbname               string
	mu                   sync.Mutex
	wg                   sync.WaitGroup
	cond                 *sync.Cond
	shutdown             atomic.Bool
	nextHead             hash.Hash
	lastPushedHead       hash.Hash
	nextPushAttempt      time.Time
	nextHeadIncomingTime time.Time
	lastSuccess          time.Time
	currentError         *string
	cancelReplicate      func()
	sqlCtxFactory        SqlContextFactory

	// waitNotify is set by controller when it needs to track whether the
	// commithooks are caught up with replicating to the standby.
	waitNotify func()

	// |mu| must be held for all accesses.
	progressNotifier ProgressNotifier

	// If this is true, the waitF returned by Execute() will fast fail if
	// we are not already caught up, instead of blocking on a successCh
	// actually indicated we are caught up. This is set to by a call to
	// NotifyWaitFailed(), an optional interface on CommitHook.
	fastFailReplicationWait bool

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

func newCommitHook(lgr *logrus.Logger, remotename, remoteurl, dbname string, role Role, destDBF func(context.Context) (*doltdb.DoltDB, error), srcDB *doltdb.DoltDB, tempDir string) *commithook {
	var ret commithook
	ret.rootLgr = lgr.WithField(logFieldThread, "Standby Replication - "+dbname+" to "+remotename)
	ret.lgr.Store(ret.rootLgr.WithField(logFieldRole, string(role)))
	ret.remotename = remotename
	ret.remoteurl = remoteurl
	ret.dbname = dbname
	ret.role = role
	ret.destDBF = destDBF
	ret.srcDB = srcDB
	ret.tempDir = tempDir
	ret.cond = sync.NewCond(&ret.mu)
	return &ret
}

func (h *commithook) Run(bt *sql.BackgroundThreads, ctxF SqlContextFactory) error {
	h.sqlCtxFactory = ctxF
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
	h.mu.Lock()
	if h.cancelReplicate != nil {
		h.cancelReplicate()
		h.cancelReplicate = nil
	}
	h.cond.Signal()
	h.mu.Unlock()
	h.wg.Wait()
	h.logger().Tracef("cluster/commithook: background thread: completed.")
}

func (h *commithook) replicate(ctx context.Context) {
	defer h.wg.Done()
	defer h.logger().Tracef("cluster/commithook: background thread: replicate: shutdown.")
	h.mu.Lock()
	defer h.mu.Unlock()
	shouldHeartbeat := false
	for !h.shutdown.Load() {
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
			func() {
				sqlCtx, err := h.sqlCtxFactory(ctx)
				if err != nil {
					lgr.Warningf("standby replication thread failed to load database root: could not create sql.Context: %v", err)
					return
				}
				defer sql.SessionEnd(sqlCtx.Session)
				sql.SessionCommandBegin(sqlCtx.Session)
				defer sql.SessionCommandEnd(sqlCtx.Session)

				// When the replicate thread comes up, it attempts to replicate the current head.
				datasDB := doltdb.HackDatasDatabaseFromDoltDB(h.srcDB)
				cs := datas.ChunkStoreFromDatabase(datasDB)
				h.nextHead, err = cs.Root(sqlCtx)
				if err != nil {
					// TODO: if err != nil, something is really wrong; should shutdown or backoff.
					lgr.Warningf("standby replication thread failed to load database root: %v", err)
					h.nextHead = hash.Hash{}
				}

				// We do not know when this head was written, but we
				// are starting to try to replicate it now.
				h.nextHeadIncomingTime = time.Now()
			}()
		} else if h.shouldReplicate() {
			h.attemptReplicate(ctx)
			shouldHeartbeat = false
		} else {
			lgr.Tracef("cluster/commithook: background thread: waiting for signal.")
			if h.waitNotify != nil {
				h.waitNotify()
			}
			caughtUp := h.isCaughtUp()
			if caughtUp {
				h.fastFailReplicationWait = false

				// If we ABA on h.nextHead, so that it gets set
				// to one value, then another, then back to the
				// first, then the setter for B can make an
				// outstanding wait while we are replicating
				// the first set to A. We can be back to
				// nextHead == A by the time we complete
				// replicating the first A and we will have the
				// outstanding waiter for the work for B but we
				// will be fully quiesced. We make sure to
				// notify B of success here.
				if h.progressNotifier.HasWaiters() {
					a := h.progressNotifier.BeginAttempt()
					h.progressNotifier.RecordSuccess(a)
				}
			}
			if shouldHeartbeat {
				h.attemptHeartbeat(ctx)

				// attemptHeartbeat releases |h.mu| for part of
				// its work. We could miss a shutdown signal
				// here, but the shutdown signal is always
				// delivered after the shared Context is
				// canceled. We check the context again here so
				// that we don't fail to shutdown if we miss a
				// shutdown signal.
				if ctx.Err() != nil {
					continue
				}
			} else if caughtUp {
				shouldHeartbeat = true
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
	if h.nextHead == (hash.Hash{}) {
		return false
	}
	return h.nextHead == h.lastPushedHead
}

// called with h.mu locked.
func (h *commithook) primaryNeedsInit() bool {
	return h.role == RolePrimary && h.nextHead == (hash.Hash{})
}

// Called by the replicate thread to periodically heartbeat liveness to a
// standby if we are a primary. These heartbeats are best effort and currently
// do not affect the data plane much.
//
// preconditions: h.mu is locked and shouldReplicate() returned false.
func (h *commithook) attemptHeartbeat(ctx context.Context) {
	if h.role != RolePrimary {
		return
	}
	head := h.lastPushedHead
	if head.IsEmpty() {
		return
	}
	destDB := h.destDB
	if destDB == nil {
		return
	}
	ctx, h.cancelReplicate = context.WithTimeout(ctx, 5*time.Second)
	defer func() {
		if h.cancelReplicate != nil {
			h.cancelReplicate()
		}
		h.cancelReplicate = nil
	}()
	// We do not take a sql.Context here. Our
	// sql Session lifecycle events are for
	// accessing srcDB, not destDB.
	h.mu.Unlock()
	datasDB := doltdb.HackDatasDatabaseFromDoltDB(destDB)
	cs := datas.ChunkStoreFromDatabase(datasDB)
	cs.Commit(ctx, head, head)
	h.mu.Lock()
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
	attempt := h.progressNotifier.BeginAttempt()
	defer h.progressNotifier.RecordFailure(attempt)
	h.mu.Unlock()

	sqlCtx, err := h.sqlCtxFactory(ctx)
	if err != nil {
		h.mu.Lock()
		h.currentError = new(string)
		*h.currentError = fmt.Sprintf("could not replicate to standby: error creating sql.Context: %v.", err)
		lgr.Warnf("cluster/commithook: could not replicate to standby: error creating sql.Context: %v.", err)
		if toPush == h.nextHead {
			h.nextPushAttempt = time.Now().Add(1 * time.Second)
		}
		return
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)

	if destDB == nil {
		lgr.Tracef("cluster/commithook: attempting to fetch destDB.")
		var err error
		destDB, err = h.destDBF(sqlCtx)
		if err != nil {
			h.mu.Lock()
			h.currentError = new(string)
			*h.currentError = fmt.Sprintf("could not replicate to standby: error fetching destDB: %v", err)
			lgr.Warnf("cluster/commithook: could not replicate to standby: error fetching destDB: %v.", err)
			// TODO: We could add some backoff here.
			if toPush == h.nextHead {
				h.nextPushAttempt = time.Now().Add(1 * time.Second)
			}
			return
		}
		lgr.Tracef("cluster/commithook: fetched destDB")
		h.mu.Lock()
		h.destDB = destDB
		h.mu.Unlock()
	}

	lgr.Tracef("cluster/commithook: pushing chunks for root hash %v to destDB", toPush.String())
	err = destDB.PullChunks(sqlCtx, h.tempDir, h.srcDB, []hash.Hash{toPush}, nil, nil)
	if err == nil {
		lgr.Tracef("cluster/commithook: successfully pushed chunks, setting root")
		datasDB := doltdb.HackDatasDatabaseFromDoltDB(destDB)
		cs := datas.ChunkStoreFromDatabase(datasDB)
		var curRootHash hash.Hash
		if err = cs.Rebase(sqlCtx); err == nil {
			if curRootHash, err = cs.Root(sqlCtx); err == nil {
				var ok bool
				ok, err = cs.Commit(sqlCtx, toPush, curRootHash)
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
			lgr.Tracef("cluster/commithook: successfully Committed chunks on destDB")
			h.lastPushedHead = toPush
			h.lastSuccess = incomingTime
			h.nextPushAttempt = time.Time{}
			h.progressNotifier.RecordSuccess(attempt)
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
	for !h.shutdown.Load() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.cond.Signal()
		}
	}
}

func (h *commithook) databaseWasDropped() {
	h.shutdown.Store(true)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancelReplicate != nil {
		h.cancelReplicate()
		h.cancelReplicate = nil
	}
	h.cond.Signal()
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

func (h *commithook) setWaitNotify(f func()) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if f != nil {
		if h.waitNotify != nil {
			return false
		}
		f()
	}
	h.waitNotify = f
	return true
}

var errDetectedBrokenConfigStr = "error: more than one server was configured as primary in the same epoch. this server has stopped accepting writes. choose a primary in the cluster and call dolt_assume_cluster_role() on servers in the cluster to start replication at a higher epoch"

// Execute on this commithook updates the target root hash we're attempting to
// replicate and wakes the replication thread.
func (h *commithook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	lgr := h.logger()
	lgr.Tracef("cluster/commithook: Execute called post commit")
	root, err := db.NomsRoot(ctx)
	if err != nil {
		lgr.Errorf("cluster/commithook: Execute: error retrieving local database root: %v", err)
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	lgr = h.logger()
	if h.role != RolePrimary {
		lgr.Warnf("cluster/commithook received commit callback for a commit on %s, but we are not role primary; not replicating the commit, which is likely to be lost.", ds.ID())
		return nil, nil
	}
	if root != h.nextHead {
		lgr.Tracef("signaling replication thread to push new head: %v", root.String())
		h.nextHeadIncomingTime = time.Now()
		h.nextHead = root
		h.nextPushAttempt = time.Time{}
		h.cond.Signal()
	}
	var waitF func(context.Context) error
	if !h.isCaughtUp() {
		if h.fastFailReplicationWait {
			waitF = func(ctx context.Context) error {
				return fmt.Errorf("circuit breaker for replication to %s/%s is open. this commit did not necessarily replicate successfully.", h.remotename, h.dbname)
			}
		} else {
			waitF = h.progressNotifier.Wait()
		}
	}
	return waitF, nil
}

func (h *commithook) NotifyWaitFailed() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fastFailReplicationWait = true
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
