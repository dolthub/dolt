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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

type remotesrvStore struct {
	ctx      *sql.Context
	readonly bool
}

var _ remotesrv.DBCache = remotesrvStore{}

func (s remotesrvStore) Get(path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	sess := dsess.DSessFromSess(s.ctx.Session)
	db, err := sess.Provider().Database(s.ctx, path)
	if err != nil {
		if !s.readonly && sql.ErrDatabaseNotFound.Is(err) {
			err = sess.Provider().CreateDatabase(s.ctx, path)
			if err != nil {
				return nil, err
			}
			db, err = sess.Provider().Database(s.ctx, path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	sdb, ok := db.(SqlDatabase)
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

func RemoteSrvServerArgs(ctx *sql.Context, args remotesrv.ServerArgs) remotesrv.ServerArgs {
	sess := dsess.DSessFromSess(ctx.Session)
	args.FS = sess.Provider().FileSystem()
	args.DBCache = remotesrvStore{ctx, args.ReadOnly}
	return args
}
