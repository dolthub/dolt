// Copyright 2021 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/types"
)

type ReadReplicaDatabase struct {
	Database
	headRef        ref.DoltRef
	remoteTrackRef ref.DoltRef
	remote         env.Remote
	srcDB          *doltdb.DoltDB
	tmpDir         string
}

var _ SqlDatabase = ReadReplicaDatabase{}
var _ sql.VersionedDatabase = ReadReplicaDatabase{}
var _ sql.TableDropper = ReadReplicaDatabase{}
var _ sql.TableCreator = ReadReplicaDatabase{}
var _ sql.TemporaryTableCreator = ReadReplicaDatabase{}
var _ sql.TableRenamer = ReadReplicaDatabase{}
var _ sql.TriggerDatabase = &ReadReplicaDatabase{}
var _ sql.StoredProcedureDatabase = ReadReplicaDatabase{}
var _ sql.TransactionDatabase = ReadReplicaDatabase{}

var ErrFailedToLoadReplicaDB = errors.New("failed to load replica database")

var EmptyReadReplica = ReadReplicaDatabase{}

func NewReadReplicaDatabase(ctx context.Context, db Database, remoteName string, rsr env.RepoStateReader, tmpDir string) (ReadReplicaDatabase, error) {
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return EmptyReadReplica, err
	}

	remote, ok := remotes[remoteName]
	if !ok {
		return EmptyReadReplica, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	srcDB, err := remote.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		return EmptyReadReplica, err
	}

	headRef := rsr.CWBHeadRef()
	refSpecs, err := env.GetRefSpecs(rsr, remoteName)

	var remoteTrackRef ref.DoltRef
	var foundRef bool
	for _, refSpec := range refSpecs {
		trackRef := refSpec.DestRef(headRef)
		if trackRef != nil {
			remoteTrackRef = trackRef
			foundRef = true
			break
		}
	}
	if !foundRef {
		return EmptyReadReplica, env.ErrInvalidRefSpecRemote
	}

	return ReadReplicaDatabase{
		Database:       db,
		headRef:        headRef,
		remoteTrackRef: remoteTrackRef,
		remote:         remote,
		tmpDir:         tmpDir,
		srcDB:          srcDB,
	}, nil
}

func (rrd ReadReplicaDatabase) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	if rrd.srcDB != nil {
		err := rrd.pullFromReplica(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		ctx.GetLogger().Warn("replication failure; dolt_replication_remote value is misconfigured")
	}
	return rrd.Database.StartTransaction(ctx, tCharacteristic)
}

func (rrd ReadReplicaDatabase) pullFromReplica(ctx *sql.Context) error {
	ddb := rrd.Database.DbData().Ddb
	err := rrd.srcDB.Rebase(ctx)
	if err != nil {
		return err
	}
	srcDBCommit, err := actions.FetchRemoteBranch(ctx, rrd.tmpDir, rrd.remote, rrd.srcDB, ddb, rrd.headRef, nil, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		return err
	}

	err = ddb.FastForward(ctx, rrd.remoteTrackRef, srcDBCommit)
	if err != nil {
		return err
	}

	err = ddb.FastForward(ctx, rrd.headRef, srcDBCommit)
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(rrd.headRef)
	if err != nil {
		return err
	}

	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	commitRoot, err := srcDBCommit.GetRootValue()
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)

	h, err := ws.HashOf()
	if err != nil {
		return err
	}
	rrd.ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, h, doltdb.TodoWorkingSetMeta())

	return nil
}
