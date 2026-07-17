// Copyright 2023 Dolthub, Inc.
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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/sirupsen/logrus"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type MySQLDbPersister interface {
	mysql_db.MySQLDbPersistence
	LoadData(context.Context) ([]byte, error)
}

type replicatingMySQLDbPersister struct {
	base     MySQLDbPersister
	current  []byte
	replicas []*mysqlDbReplica

	mu      sync.Mutex
	version uint32
}

type mysqlDbReplica struct {
	nextAttempt             time.Time
	backoff                 backoff.BackOff
	waitNotify              func()
	client                  *replicationServiceClient
	lgr                     *logrus.Entry
	cond                    *sync.Cond
	role                    Role
	contents                []byte
	progressNotifier        ProgressNotifier
	mu                      sync.Mutex
	version                 uint32
	replicatedVersion       uint32
	shutdown                bool
	fastFailReplicationWait bool
}

func (r *mysqlDbReplica) UpdateMySQLDb(ctx context.Context, contents []byte, version uint32) func(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lgr.Infof("cluster/trace[mysqlDbReplica %s]: ts=%s got new contents at version %d; %s", r.client.remote, tsNow(), version, r.debugStateLocked())
	r.contents = contents
	r.version = version
	r.nextAttempt = time.Time{}
	r.backoff.Reset()
	// See setWaitNotify: the caller may block on this update replicating, so also allow
	// the grpc channel to attempt a fresh connection immediately.
	if r.client.conn != nil {
		r.client.conn.ResetConnectBackoff()
	}
	r.cond.Broadcast()

	if r.fastFailReplicationWait {
		remote := r.client.remote
		return func(ctx context.Context) error {
			return fmt.Errorf("circuit breaker for replication to %s/mysql is open. this update to users and grants did not necessarily replicate successfully.", remote)
		}
	} else {
		w := r.progressNotifier.Wait()
		return func(ctx context.Context) error {
			err := w(ctx)
			if err != nil && errors.Is(err, doltdb.ErrReplicationWaitFailed) {
				r.setFastFailReplicationWait(true)
			}
			return err
		}
	}
}

func (r *mysqlDbReplica) setFastFailReplicationWait(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fastFailReplicationWait = v
}

func (r *mysqlDbReplica) Run() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lgr.Tracef("mysqlDbReplica[%s]: running", r.client.remote)
	defer r.client.closer()
	for !r.shutdown {
		if r.role != RolePrimary {
			r.wait()
			continue
		}
		if r.version == 0 {
			r.wait()
			continue
		}
		if r.replicatedVersion == r.version {
			r.wait()
			continue
		}
		if r.nextAttempt.After(time.Now()) {
			r.wait()
			continue
		}
		if len(r.contents) > 0 {
			// We do not call into the client with the lock held
			// here.  Client interceptors could call
			// `controller.setRoleAndEpoch()`, which will call back
			// into this replica with the new role. We need to
			// release this lock in order to avoid deadlock.
			contents := r.contents
			client := r.client.client
			version := r.version
			attempt := r.progressNotifier.BeginAttempt()
			r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s attempting UpdateUsersAndGrants; %s", r.client.remote, tsNow(), r.debugStateLocked())
			attemptStart := time.Now()
			r.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err := client.UpdateUsersAndGrants(ctx, &replicationapi.UpdateUsersAndGrantsRequest{
				SerializedContents: contents,
			})
			cancel()
			r.mu.Lock()
			if err != nil {
				r.progressNotifier.RecordFailure(attempt)
				r.lgr.Warnf("cluster/trace[mysqlDbReplica %s]: ts=%s error replicating users and grants (attempt took %v, connState now %s). backing off. %v", r.client.remote, tsNow(), time.Since(attemptStart), connStateStr(r.client.conn), err)
				if r.waitNotify != nil {
					// Someone (e.g. a graceful role transition) is blocked on this replica
					// catching up, on a fixed budget. Retry on a short flat cadence with a
					// fresh connection attempt each time, rather than letting the growing
					// backoff (both ours and the grpc channel's internal reconnect backoff)
					// idle out their entire wait.
					r.nextAttempt = time.Now().Add(time.Second)
					if r.client.conn != nil {
						r.client.conn.ResetConnectBackoff()
					}
					r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s waiter present: flat 1s retry scheduled, connect backoff reset", r.client.remote, tsNow())
				} else {
					r.nextAttempt = time.Now().Add(r.backoff.NextBackOff())
					r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s no waiter: exponential backoff, next attempt at %s", r.client.remote, tsNow(), r.nextAttempt.Format("15:04:05.000000"))
				}
				next := r.nextAttempt
				go func() {
					<-time.After(time.Until(next))
					r.mu.Lock()
					defer r.mu.Unlock()
					if r.nextAttempt == next {
						r.nextAttempt = time.Time{}
					}
					r.cond.Broadcast()
				}()
				continue
			}
			r.progressNotifier.RecordSuccess(attempt)
			r.fastFailReplicationWait = false
			r.backoff.Reset()
			r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s successfully replicated users and grants at version %d (attempt took %v).", r.client.remote, tsNow(), version, time.Since(attemptStart))
			r.replicatedVersion = version
		} else {
			r.lgr.Debugf("mysqlDbReplica[%s]: not replicating empty users and grants at version %d.", r.client.remote, r.version)
			r.replicatedVersion = r.version
		}
	}
}

func (r *mysqlDbReplica) isCaughtUp() bool {
	return r.version == r.replicatedVersion || r.role != RolePrimary
}

// debugStateLocked renders the replica's replication-relevant state for the
// cluster/trace diagnostics. called with r.mu locked.
func (r *mysqlDbReplica) debugStateLocked() string {
	return fmt.Sprintf("role=%s version=%d replicatedVersion=%d caughtUp=%v nextAttempt=%s waiterPresent=%v fastFail=%v connState=%s",
		r.role, r.version, r.replicatedVersion, r.isCaughtUp(), r.nextAttempt.Format("15:04:05.000000"), r.waitNotify != nil, r.fastFailReplicationWait, connStateStr(r.client.conn))
}

func (r *mysqlDbReplica) setWaitNotify(notify func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if notify != nil {
		if r.waitNotify != nil {
			r.lgr.Warnf("cluster/trace[mysqlDbReplica %s]: ts=%s setWaitNotify: rejected; a waiter is already registered", r.client.remote, tsNow())
			return false
		}
		r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s setWaitNotify: waiter registered; %s", r.client.remote, tsNow(), r.debugStateLocked())
		notify()
		// A waiter has just registered: someone (e.g. a graceful role transition) is now
		// blocked on this replica catching up, on a fixed budget. If earlier attempts
		// failed — common on first contact, while the peer is still coming up — the
		// accumulated backoff can schedule the next attempt beyond that entire budget.
		// Prod the run loop to retry immediately with a fresh backoff instead. The grpc
		// channel maintains its own internal reconnect backoff, which would otherwise
		// keep RPCs failing fast on the cached connection error without dialing, so
		// reset it as well.
		if !r.isCaughtUp() {
			r.nextAttempt = time.Time{}
			r.backoff.Reset()
			if r.client.conn != nil {
				r.client.conn.ResetConnectBackoff()
			}
			r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s setWaitNotify: not caught up; backoffs reset and run loop prodded", r.client.remote, tsNow())
			r.cond.Broadcast()
		}
	} else if r.waitNotify != nil {
		r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s setWaitNotify: waiter unregistered; %s", r.client.remote, tsNow(), r.debugStateLocked())
	}
	r.waitNotify = notify
	return true
}

func (r *mysqlDbReplica) wait() {
	if r.waitNotify != nil {
		r.waitNotify()
	}
	r.lgr.Infof("cluster/trace[mysqlDbReplica %s]: ts=%s run loop waiting; %s", r.client.remote, tsNow(), r.debugStateLocked())
	if r.isCaughtUp() {
		attempt := r.progressNotifier.BeginAttempt()
		r.progressNotifier.RecordSuccess(attempt)
	}
	r.cond.Wait()
}

func (r *mysqlDbReplica) GracefulStop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdown = true
	r.cond.Broadcast()
}

func (r *mysqlDbReplica) setRole(role Role) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lgr.Tracef("cluster/trace[mysqlDbReplica %s]: ts=%s setRole %s -> %s", r.client.remote, tsNow(), r.role, role)
	r.role = role
	r.nextAttempt = time.Time{}
	r.backoff.Reset()
	r.cond.Broadcast()
}

func (p *replicatingMySQLDbPersister) setRole(role Role) {
	for _, r := range p.replicas {
		r.setRole(role)
	}
	p.mu.Lock()
	// If we are transitioning to primary and we are already initialized,
	// then we reload data so that we have the most recent persisted users
	// and grants to replicate.
	needsLoad := p.version != 0 && role == RolePrimary
	p.mu.Unlock()
	if needsLoad {
		p.LoadData(context.Background())
	}
}

func (p *replicatingMySQLDbPersister) Run() {
	var wg sync.WaitGroup
	for _, r := range p.replicas {
		wg.Go(r.Run)
	}
	wg.Wait()
}

func (p *replicatingMySQLDbPersister) GracefulStop() {
	for _, r := range p.replicas {
		r.GracefulStop()
	}
}

func (p *replicatingMySQLDbPersister) Persist(ctx *sql.Context, data []byte) error {
	p.mu.Lock()
	err := p.base.Persist(ctx, data)
	if err == nil {
		p.current = data
		p.version += 1
		var rsc doltdb.ReplicationStatusController
		rsc.Wait = make([]func(context.Context) error, len(p.replicas))
		rsc.NotifyWaitFailed = make([]func(), len(p.replicas))
		for i, r := range p.replicas {
			rsc.Wait[i] = r.UpdateMySQLDb(ctx, p.current, p.version)
			rsc.NotifyWaitFailed[i] = func() {}
		}
		p.mu.Unlock()
		dsess.WaitForReplicationController(ctx, rsc)
	} else {
		p.mu.Unlock()
	}
	return err
}

func (p *replicatingMySQLDbPersister) LoadData(ctx context.Context) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret, err := p.base.LoadData(ctx)
	if err == nil {
		p.current = ret
		p.version += 1
		for _, r := range p.replicas {
			r.UpdateMySQLDb(ctx, p.current, p.version)
		}
	}
	return ret, err
}

func (p *replicatingMySQLDbPersister) waitForReplication(timeout time.Duration) ([]graceTransitionResult, error) {
	p.mu.Lock()
	replicas := make([]*mysqlDbReplica, len(p.replicas))
	copy(replicas, p.replicas)
	res := make([]graceTransitionResult, len(replicas))
	for i := range replicas {
		res[i].database = "mysql"
		res[i].remote = replicas[i].client.remote
		res[i].remoteUrl = replicas[i].client.httpUrl
	}
	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for i, r := range replicas {
		ok := r.setWaitNotify(func() {
			// called with r.mu locked.
			if !res[i].caughtUp {
				if r.isCaughtUp() {
					res[i].caughtUp = true
					wg.Done()
				} else {
				}
			}
		})
		if !ok {
			for j := i - 1; j >= 0; j-- {
				replicas[j].setWaitNotify(nil)
			}
			p.mu.Unlock()
			return nil, errors.New("cluster: mysqldb replication: could not wait for replication. Concurrent waiters conflicted with each other.")
		}
	}
	p.mu.Unlock()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		if len(replicas) > 0 {
			replicas[0].lgr.Tracef("cluster/trace[mysqlDbPersister]: ts=%s waitForReplication: all %d replicas caught up after %v", tsNow(), len(replicas), time.Since(start))
		}
	case <-time.After(timeout):
		if len(replicas) > 0 {
			replicas[0].lgr.Warnf("cluster/trace[mysqlDbPersister]: ts=%s waitForReplication: TIMED OUT after %v", tsNow(), time.Since(start))
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for i, r := range replicas {
		if !res[i].caughtUp {
			r.lgr.Warnf("cluster/trace[mysqlDbPersister]: ts=%s waitForReplication: replica %s not caught up at deadline", tsNow(), r.client.remote)
		}
		r.setWaitNotify(nil)
	}

	// Make certain we don't leak the wg.Wait goroutine in the failure case.
	// At this point, none of the callbacks will ever be called again and
	// ch.setWaitNotify grabs a lock and so establishes the happens before.
	for _, b := range res {
		if !b.caughtUp {
			wg.Done()
		}
	}
	<-done

	return res, nil
}
