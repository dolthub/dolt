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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func getPushOnWriteHook(ctx context.Context, bThreads *sql.BackgroundThreads, dEnv *env.DoltEnv, logger io.Writer) (doltdb.CommitHook, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateToRemote)
	if !ok {
		return nil, sql.ErrUnknownSystemVariable.New(dsess.ReplicateToRemote)
	} else if val == "" {
		return nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes[remoteName]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return nil, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	if _, val, ok = sql.SystemVariables.GetGlobal(dsess.AsyncReplication); ok && val == dsess.SysVarTrue {
		return doltdb.NewAsyncPushOnWriteHook(bThreads, ddb, tmpDir, logger)
	}

	return doltdb.NewPushOnWriteHook(ddb, tmpDir), nil
}

// GetCommitHooks creates a list of hooks to execute on database commit. If doltdb.SkipReplicationErrorsKey is set,
// replace misconfigured hooks with doltdb.LogHook instances that prints a warning when trying to execute.
func GetCommitHooks(ctx context.Context, bThreads *sql.BackgroundThreads, dEnv *env.DoltEnv, logger io.Writer) ([]doltdb.CommitHook, error) {
	postCommitHooks := make([]doltdb.CommitHook, 0)

	if hook, err := getPushOnWriteHook(ctx, bThreads, dEnv, logger); err != nil {
		path, _ := dEnv.FS.Abs(".")
		err = fmt.Errorf("failure loading hook for database at %s; %w", path, err)
		if dsess.IgnoreReplicationErrors() {
			postCommitHooks = append(postCommitHooks, doltdb.NewLogHook([]byte(err.Error()+"\n")))
		} else {
			return nil, err
		}
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	for _, h := range postCommitHooks {
		h.SetLogger(ctx, logger)
	}
	return postCommitHooks, nil
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (ReadReplicaDatabase, error) {
	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}

	db, err := NewDatabase(ctx, name, dEnv.DbData(), opts)
	if err != nil {
		return ReadReplicaDatabase{}, err
	}

	rrd, err := NewReadReplicaDatabase(ctx, db, remoteName, dEnv)
	if err != nil {
		err = fmt.Errorf("%w from remote '%s'; %s", ErrFailedToLoadReplicaDB, remoteName, err.Error())
		if !dsess.IgnoreReplicationErrors() {
			return ReadReplicaDatabase{}, err
		}
		cli.Println(err)
		return ReadReplicaDatabase{Database: db}, nil
	}
	return rrd, nil
}

func ApplyReplicationConfig(ctx context.Context, bThreads *sql.BackgroundThreads, mrEnv *env.MultiRepoEnv, logger io.Writer, dbs ...SqlDatabase) ([]SqlDatabase, error) {
	outputDbs := make([]SqlDatabase, len(dbs))
	for i, db := range dbs {
		dEnv := mrEnv.GetEnv(db.Name())
		if dEnv == nil {
			outputDbs = append(outputDbs, db)
			continue
		}
		postCommitHooks, err := GetCommitHooks(ctx, bThreads, dEnv, logger)
		if err != nil {
			return nil, err
		}
		dEnv.DoltDB.SetCommitHooks(ctx, postCommitHooks)

		if _, remote, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaRemote); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return nil, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			db, err = newReplicaDatabase(ctx, db.Name(), remoteName, dEnv)
			if err != nil {
				return nil, err
			}
		}

		outputDbs[i] = db
	}
	return outputDbs, nil
}
