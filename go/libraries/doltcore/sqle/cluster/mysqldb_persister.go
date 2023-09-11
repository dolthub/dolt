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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/sirupsen/logrus"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
)

type MySQLDbPersister interface {
	mysql_db.MySQLDbPersistence
	LoadData(context.Context) ([]byte, error)
}

type replicatingPersister struct {
	base MySQLDbPersister

	current  []byte
	version  uint32
	replicas []*mysqlDbReplica

	mu sync.Mutex
}

type mysqlDbReplica struct {
	shutdown bool
	role     Role

	contents []byte
	version  uint32

	replicatedVersion uint32
	backoff           backoff.BackOff
	nextAttempt       time.Time

	client *replicationServiceClient
	lgr    *logrus.Entry

	mu   sync.Mutex
	cond *sync.Cond
}

func (r *mysqlDbReplica) UpdateMySQLDb(ctx context.Context, contents []byte, version uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contents = contents
	r.version = version
	r.nextAttempt = time.Time{}
	r.backoff.Reset()
	r.cond.Broadcast()
	return nil
}

func (r *mysqlDbReplica) Run() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lgr.Tracef("mysqlDbReplica[%s]: running", r.client.remote)
	for !r.shutdown {
		if r.role != RolePrimary {
			r.cond.Wait()
			continue
		}
		if r.version == 0 {
			r.cond.Wait()
			continue
		}
		if r.replicatedVersion == r.version {
			r.cond.Wait()
			continue
		}
		if r.nextAttempt.After(time.Now()) {
			r.cond.Wait()
			continue
		}
		if len(r.contents) > 0 {
			_, err := r.client.client.UpdateUsersAndGrants(context.Background(), &replicationapi.UpdateUsersAndGrantsRequest{
				SerializedContents: r.contents,
			})
			if err != nil {
				r.lgr.Warnf("mysqlDbReplica[%s]: error replicating users and grants. backing off. %v", r.client.remote, err)
				r.nextAttempt = time.Now().Add(r.backoff.NextBackOff())
				next := r.nextAttempt
				go func() {
					<-time.After(time.Until(next))
					r.mu.Lock()
					defer r.mu.Unlock()
					for !time.Now().After(next) {
					}
					r.cond.Broadcast()
				}()
				continue
			}
			r.backoff.Reset()
			r.lgr.Debugf("mysqlDbReplica[%s]: sucessfully replicated users and grants.", r.client.remote)
		} else {
			r.lgr.Debugf("mysqlDbReplica[%s]: not replicating empty users and grants.", r.client.remote)
		}
		r.replicatedVersion = r.version
	}
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
	r.role = role
	r.cond.Broadcast()
}

func (p *replicatingPersister) setRole(role Role) {
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

func (p *replicatingPersister) Run() {
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

func (p *replicatingPersister) GracefulStop() {
	for _, r := range p.replicas {
		r.GracefulStop()
	}
}

func (p *replicatingPersister) Persist(ctx *sql.Context, data []byte) error {
	err := p.base.Persist(ctx, data)
	if err == nil {
		p.mu.Lock()
		p.current = data
		p.version += 1
		defer p.mu.Unlock()
		for _, r := range p.replicas {
			r.UpdateMySQLDb(ctx, p.current, p.version)
		}
	}
	return err
}

func (p *replicatingPersister) LoadData(ctx context.Context) ([]byte, error) {
	ret, err := p.base.LoadData(ctx)
	if err == nil {
		p.mu.Lock()
		p.current = ret
		p.version += 1
		defer p.mu.Unlock()
		for _, r := range p.replicas {
			r.UpdateMySQLDb(ctx, p.current, p.version)
		}
	}
	return ret, err
}

type replicationServiceServer struct {
	replicationapi.UnimplementedReplicationServiceServer
	mysqlDb *mysql_db.MySQLDb
}

func (s *replicationServiceServer) UpdateUsersAndGrants(ctx context.Context, req *replicationapi.UpdateUsersAndGrantsRequest) (*replicationapi.UpdateUsersAndGrantsResponse, error) {
	sqlCtx := sql.NewContext(ctx)
	ed := s.mysqlDb.Editor()
	defer ed.Close()
	err := s.mysqlDb.OverwriteUsersAndGrantData(sqlCtx, ed, req.SerializedContents)
	if err != nil {
		return nil, err
	}
	err = s.mysqlDb.Persist(sqlCtx, ed)
	if err != nil {
		return nil, err
	}
	return &replicationapi.UpdateUsersAndGrantsResponse{}, nil
}
