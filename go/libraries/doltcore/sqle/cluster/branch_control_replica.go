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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
)

type branchControlReplication struct {
	current  []byte
	version  uint32
	replicas []*branchControlReplica

	bcController *branch_control.Controller

	mu sync.Mutex
}

type branchControlReplica struct {
	shutdown bool
	role     Role

	contents          []byte
	version           uint32
	replicatedVersion uint32

	backoff     backoff.BackOff
	nextAttempt time.Time

	client *replicationServiceClient
	lgr    *logrus.Entry

	waitNotify func()

	mu   sync.Mutex
	cond *sync.Cond
}

func (r *branchControlReplica) UpdateContents(contents []byte, version uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contents = contents
	r.version = version
	r.nextAttempt = time.Time{}
	r.backoff.Reset()
	r.cond.Broadcast()
}

func (r *branchControlReplica) Run() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lgr.Tracef("branchControlReplica[%s]: running", r.client.remote)
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
		// We do not call into the client with the lock held here.
		// Client interceptors could call
		// `controller.setRoleAndEpoch()`, which will call back into
		// this replica with the new role. We need to release this lock
		// in order to avoid deadlock.
		contents := r.contents
		client := r.client.client
		version := r.version
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := client.UpdateBranchControl(ctx, &replicationapi.UpdateBranchControlRequest{
			SerializedContents: contents,
		})
		cancel()
		r.mu.Lock()
		if err != nil {
			r.lgr.Warnf("branchControlReplica[%s]: error replicating branch control permissions. backing off. %v", r.client.remote, err)
			r.nextAttempt = time.Now().Add(r.backoff.NextBackOff())
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
		r.backoff.Reset()
		r.lgr.Debugf("branchControlReplica[%s]: sucessfully replicated branch control permissions.", r.client.remote)
		r.replicatedVersion = version
	}
}

func (r *branchControlReplica) wait() {
	r.cond.Wait()
}

func (r *branchControlReplica) isCaughtUp() bool {
	return r.version == r.replicatedVersion || r.role != RolePrimary
}

func (r *branchControlReplica) setWaitNotify(notify func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if notify != nil {
		if r.waitNotify != nil {
			return false
		}
		notify()
	}
	r.waitNotify = notify
	return true
}

func (r *branchControlReplica) GracefulStop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdown = true
	r.cond.Broadcast()
}

func (r *branchControlReplica) setRole(role Role) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.role = role
	r.nextAttempt = time.Time{}
	r.cond.Broadcast()
}

func (p *branchControlReplication) setRole(role Role) {
	if role == RolePrimary {
		cur := p.bcController.Serialized.Load()
		if cur == nil {
			p.UpdateBranchControlContents([]byte{})
		} else {
			p.UpdateBranchControlContents(*cur)
		}
	}
	for _, r := range p.replicas {
		r.setRole(role)
	}
}

func (p *branchControlReplication) Run() {
	var wg sync.WaitGroup
	for _, r := range p.replicas {
		r := r
		wg.Add(1)
		func() {
			defer wg.Done()
			r.Run()
		}()
	}
	wg.Wait()
}

func (p *branchControlReplication) GracefulStop() {
	for _, r := range p.replicas {
		r.GracefulStop()
	}
}

func (p *branchControlReplication) UpdateBranchControlContents(contents []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = contents
	p.version += 1
	for _, r := range p.replicas {
		r.UpdateContents(p.current, p.version)
	}
}

func (p *branchControlReplication) waitForReplication(timeout time.Duration) bool {
	p.mu.Lock()
	replicas := make([]*branchControlReplica, len(p.replicas))
	copy(replicas, p.replicas)
	caughtup := make([]bool, len(replicas))
	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for li, lr := range replicas {
		i := li
		r := lr
		ok := r.setWaitNotify(func() {
			// called with r.mu locked.
			if !caughtup[i] {
				if r.isCaughtUp() {
					caughtup[i] = true
					wg.Done()
				} else {
				}
			}
		})
		if !ok {
			for j := li - 1; j >= 0; j-- {
				replicas[j].setWaitNotify(nil)
			}
			return false
		}
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range replicas {
		r.setWaitNotify(nil)
	}

	// Make certain we don't leak the wg.Wait goroutine in the failure case.
	// At this point, none of the callbacks will ever be called again and
	// ch.setWaitNotify grabs a lock and so establishes the happens before.
	all := true
	for _, b := range caughtup {
		if !b {
			wg.Done()
			all = false
		}
	}
	<-done

	return all
}
