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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

type remotesrvStore struct {
	ctxFactory func(context.Context) (*sql.Context, error)
	readonly   bool
	createDBs  bool
}

var _ remotesrv.DBCache = remotesrvStore{}

func (s remotesrvStore) Get(ctx context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	sqlCtx, err := s.ctxFactory(ctx)
	if err != nil {
		return nil, err
	}
	sess := dsess.DSessFromSess(sqlCtx.Session)
	db, err := sess.Provider().Database(sqlCtx, path)
	if err != nil {
		if !s.readonly && sql.ErrDatabaseNotFound.Is(err) && s.createDBs {
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
	datasdb := doltdb.HackDatasDatabaseFromDoltDB(sdb.DbData().Ddb)
	cs := datas.ChunkStoreFromDatabase(datasdb)
	rss, ok := cs.(remotesrv.RemoteSrvStore)
	if !ok {
		return nil, remotesrv.ErrUnimplemented
	}
	return rss, nil
}

func RemoteSrvServerArgs(ctxFactory func(context.Context) (*sql.Context, error), args remotesrv.ServerArgs) (remotesrv.ServerArgs, error) {
	sqlCtx, err := ctxFactory(context.Background())
	if err != nil {
		return remotesrv.ServerArgs{}, err
	}
	sess := dsess.DSessFromSess(sqlCtx.Session)
	args.FS = sess.Provider().FileSystem()
	args.DBCache = remotesrvStore{ctxFactory, args.ReadOnly, args.CreateUnknownDatabases}
	return args, nil
}

func WithUserPasswordAuth(args remotesrv.ServerArgs, authnz remotesrv.AccessControl) remotesrv.ServerArgs {
	si := remotesrv.ServerInterceptor{
		Lgr:              args.Logger,
		AccessController: authnz,
	}
	args.Options = append(args.Options, si.Options()...)
	return args
}
