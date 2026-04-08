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

package sqle

import (
	"context"
	"errors"
	"net/http"

	"github.com/dolthub/go-mysql-server/sql"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

type remotesrvStore struct {
	ctxFactory   func(context.Context) (*sql.Context, error)
	createDBs    bool
	replicaWrite bool
}

var _ remotesrv.DBCache = remotesrvStore{}

func (s remotesrvStore) Get(ctx context.Context, path, _ string) (remotesrv.RemoteSrvStore, error) {
	sqlCtx, err := s.ctxFactory(ctx)
	if err != nil {
		return nil, err
	}
	sess := dsess.DSessFromSess(sqlCtx.Session)
	db, err := sess.Provider().Database(sqlCtx, path)
	if err != nil {
		if s.createDBs && sql.ErrDatabaseNotFound.Is(err) {
			err = sess.Provider().CreateDatabase(sqlCtx, path)
			if err != nil {
				return nil, err
			}
			db, err = sess.Provider().Database(sqlCtx, path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	sdb, ok := db.(dsess.SqlDatabase)
	if !ok {
		return nil, remotesrv.ErrUnimplemented
	}
	ddb := sdb.DbData().Ddb
	datasdb := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(datasdb)
	rss, ok := cs.(remotesrv.RemoteSrvStore)
	if !ok {
		return nil, remotesrv.ErrUnimplemented
	}
	return hooksFiringRemoteSrvStore{RemoteSrvStore: rss, ddb: ddb, replicaWrite: s.replicaWrite}, nil
}

// hooksFiringRemoteSrvStore wraps a RemoteSrvStore and fires CommitHooks
// registered on the associated DoltDB after a successful Commit. This is used
// when sql-server exposes a remotesapi endpoint so that pushes from remote
// clients trigger the same hooks (replication, stats, etc.) as writes through
// the SQL engine.
//
// When replicaWrite is true, only hooks that return true from
// ExecuteForReplicaWrite() are fired. This is used for cluster replication
// writes on a standby replica, where replication hooks should not fire.
type hooksFiringRemoteSrvStore struct {
	remotesrv.RemoteSrvStore
	ddb          *doltdb.DoltDB
	replicaWrite bool
}

// Commit implements remotesrv.RemoteSrvStore. After a successful commit it
// fires the DoltDB's registered CommitHooks for every ref-typed dataset whose
// head address changed between |last| and |current|.
func (s hooksFiringRemoteSrvStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	// Snapshot old dataset head addresses before the commit.
	oldDatasets, _ := s.ddb.DatasetsByRootHash(ctx, last)

	ok, err := s.RemoteSrvStore.Commit(ctx, current, last)
	if !ok || err != nil {
		return ok, err
	}

	// Snapshot new dataset head addresses after the successful commit.
	newDatasets, err := s.ddb.DatasetsByRootHash(ctx, current)
	if err != nil {
		// Don't fail the commit; hooks are best-effort.
		return true, nil
	}

	// Build lookup maps for both old and new dataset head addresses.
	oldAddrs := make(map[string]hash.Hash)
	if oldDatasets != nil {
		_ = oldDatasets.IterAll(ctx, func(id string, addr hash.Hash) error {
			oldAddrs[id] = addr
			return nil
		})
	}
	newAddrs := make(map[string]hash.Hash)
	_ = newDatasets.IterAll(ctx, func(id string, addr hash.Hash) error {
		newAddrs[id] = addr
		return nil
	})

	executeHooks := s.ddb.ExecuteCommitHooks
	if s.replicaWrite {
		executeHooks = s.ddb.ExecuteReplicaCommitHooks
	}

	// Fire hooks for each ref-typed dataset that was added or changed.
	for id, newAddr := range newAddrs {
		if !ref.IsRef(id) {
			continue
		}
		if oldAddr, existed := oldAddrs[id]; !existed || oldAddr != newAddr {
			_ = executeHooks(ctx, id)
		}
	}

	// Fire hooks for each ref-typed dataset that was deleted.
	for id := range oldAddrs {
		if !ref.IsRef(id) {
			continue
		}
		if _, exists := newAddrs[id]; !exists {
			_ = executeHooks(ctx, id)
		}
	}

	return true, nil
}

// In the SQL context, the database provider that we use to expose the
// remotesapi interface can choose to either create a newly accessed database
// on first access or to return NotFound. Currently we allow creation in the
// cluster replication context, where the replicated database should definitely
// be made to exist and the accesses are always writes, but we disallow it in
// the exposed-as-a-remotesapi-endpoint use case, where the requested database
// may just be a typo or a configuration mistake on the part of the user.

type CreateUnknownDatabasesSetting bool

const CreateUnknownDatabases CreateUnknownDatabasesSetting = true
const DoNotCreateUnknownDatabases CreateUnknownDatabasesSetting = false

type ReplicaWriteSetting bool

const IsReplicaWrite ReplicaWriteSetting = true
const IsNotReplicaWrite ReplicaWriteSetting = false

// Returns a remotesrv.DBCache instance which will use the *sql.Context
// returned from |ctxFactory| to access a database in the session
// DatabaseProvider. When |replicaWriteSetting| is IsReplicaWrite, only hooks
// that return true from ExecuteForReplicaWrite() are fired on commits.
func RemoteSrvDBCache(ctxFactory func(context.Context) (*sql.Context, error), createSetting CreateUnknownDatabasesSetting, replicaWriteSetting ReplicaWriteSetting) (remotesrv.DBCache, error) {
	dbcache := remotesrvStore{ctxFactory: ctxFactory, createDBs: bool(createSetting), replicaWrite: bool(replicaWriteSetting)}
	return dbcache, nil
}

func WithUserPasswordAuth(args remotesrv.ServerArgs, authnz remotesrv.AccessControl) remotesrv.ServerArgs {
	si := remotesrv.ServerInterceptor{
		Lgr:              args.Logger,
		AccessController: authnz,
	}
	args.Options = append(args.Options, si.Options()...)
	return args
}

type SqlContextServerInterceptor struct {
	Factory func(context.Context) (*sql.Context, error)
}

type serverStreamWrapper struct {
	grpc.ServerStream
	ctx context.Context
}

func (s serverStreamWrapper) Context() context.Context {
	return s.ctx
}

type sqlContextInterceptorKey struct{}

func GetInterceptorSqlContext(ctx context.Context) (*sql.Context, error) {
	if v := ctx.Value(sqlContextInterceptorKey{}); v != nil {
		return v.(*sql.Context), nil
	}
	return nil, errors.New("misconfiguration; a sql.Context should always be available from the interceptor chain.")
}

func (si SqlContextServerInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		sqlCtx, err := si.Factory(ss.Context())
		if err != nil {
			return err
		}
		defer sql.SessionEnd(sqlCtx.Session)
		sql.SessionCommandBegin(sqlCtx.Session)
		defer sql.SessionCommandEnd(sqlCtx.Session)
		newCtx := context.WithValue(sqlCtx.Context, sqlContextInterceptorKey{}, sqlCtx)
		newSs := serverStreamWrapper{
			ServerStream: ss,
			ctx:          newCtx,
		}
		return handler(srv, newSs)
	}
}

func (si SqlContextServerInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		sqlCtx, err := si.Factory(ctx)
		if err != nil {
			return nil, err
		}
		defer sql.SessionEnd(sqlCtx.Session)
		sql.SessionCommandBegin(sqlCtx.Session)
		defer sql.SessionCommandEnd(sqlCtx.Session)
		newCtx := context.WithValue(sqlCtx.Context, sqlContextInterceptorKey{}, sqlCtx)
		return handler(newCtx, req)
	}
}

func (si SqlContextServerInterceptor) HTTP(existing func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		this := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			sqlCtx, err := si.Factory(ctx)
			if err != nil {
				http.Error(w, "could not initialize sql.Context", http.StatusInternalServerError)
				return
			}
			defer sql.SessionEnd(sqlCtx.Session)
			sql.SessionCommandBegin(sqlCtx.Session)
			defer sql.SessionCommandEnd(sqlCtx.Session)
			newCtx := context.WithValue(sqlCtx.Context, sqlContextInterceptorKey{}, sqlCtx)
			newReq := r.WithContext(newCtx)
			h.ServeHTTP(w, newReq)
		})
		if existing != nil {
			return existing(this)
		} else {
			return this
		}
	}
}

func (si SqlContextServerInterceptor) Options() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(si.Unary()),
		grpc.ChainStreamInterceptor(si.Stream()),
	}
}
