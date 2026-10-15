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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func getPushOnWriteHookWithRegistry(ctx context.Context, nameSuffix string, dEnv *env.DoltEnv, logger io.Writer, vars sql.SystemVariableRegistry) (doltdb.CommitHook, RunAsyncThreads, error) {
	return NewDynamicPushOnWriteHookWithRegistry(ctx, nameSuffix, dEnv, logger, vars)
}

type BackgroundThreads interface {
	Add(string, func(context.Context)) error
}

type RunAsyncThreads func(BackgroundThreads, func(context.Context) (*sql.Context, error)) error

// GetCommitHooksWithRegistry creates commit hooks that retain one engine registry.
// Invalid configuration is logged and does not prevent the server from starting.
func GetCommitHooksWithRegistry(ctx context.Context, nameSuffix string, dEnv *env.DoltEnv, logger io.Writer, vars sql.SystemVariableRegistry) ([]doltdb.CommitHook, RunAsyncThreads, error) {
	postCommitHooks := make([]doltdb.CommitHook, 0)
	hook, runThreads, err := getPushOnWriteHookWithRegistry(ctx, nameSuffix, dEnv, logger, vars)
	if err != nil {
		path, _ := dEnv.FS.Abs(".")
		logrus.Errorf("error loading replication for database at %s, replication disabled: %v", path, err)
		postCommitHooks = append(postCommitHooks, NewLogHook([]byte(err.Error()+"\n"), logger))
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	return postCommitHooks, runThreads, nil
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (ReadReplicaDatabase, error) {
	db, err := NewDatabase(ctx, name, dEnv.DbData(ctx), editor.Options{})
	if err != nil {
		return ReadReplicaDatabase{}, err
	}

	rrd, err := NewReadReplicaDatabase(ctx, db, remoteName, dEnv)
	if err != nil {
		err = fmt.Errorf("%s from remote '%s'; %w", ErrFailedToLoadReplicaDB.Error(), remoteName, err)
		return ReadReplicaDatabase{}, err
	}

	if sqlCtx, ok := ctx.(*sql.Context); ok {
		sqlCtx.GetLogger().Infof(
			"replication enabled for database '%s' from remote '%s'", name, remoteName)
	}

	return rrd, nil
}

// Converts |db| into a |ReadReplicaDatabase| if read replication is
// configured through sql SystemVariables. This is called both at
// startup, for the entire set of databases, and is called when
// we create new databases through |registerNewDatabases|.
func applyReadReplicationConfigToDatabaseWithRegistry(ctx context.Context, dEnv *env.DoltEnv, db dsess.SqlDatabase, vars sql.SystemVariableRegistry) (dsess.SqlDatabase, error) {
	if _, remote, ok := vars.GetGlobal(dsess.ReadReplicaRemote); ok && remote != "" {
		remoteName, ok := remote.(string)
		if !ok {
			return nil, sql.ErrInvalidSystemVariableValue.New(remote)
		}
		rdb, err := newReplicaDatabase(ctx, db.Name(), remoteName, dEnv)
		if err == nil {
			db = rdb
		} else {
			logrus.Errorf("invalid replication configuration, replication disabled: %v", err)
		}
	}
	return db, nil
}

// ApplyReplicationConfigWithRegistry constructs replication state using the
// supplied engine registry, including hooks that read live settings later.
func ApplyReplicationConfigWithRegistry(ctx context.Context, mrEnv *env.MultiRepoEnv, logger io.Writer, vars sql.SystemVariableRegistry, dbs ...dsess.SqlDatabase) ([]dsess.SqlDatabase, RunAsyncThreads, error) {
	outputDbs := make([]dsess.SqlDatabase, len(dbs))
	asyncRunners := make([]RunAsyncThreads, len(dbs))
	for i, db := range dbs {
		dEnv := mrEnv.GetEnv(db.Name())
		if dEnv == nil {
			outputDbs[i] = db
			continue
		}
		nameSuffix := "[" + db.Name() + "]"
		postCommitHooks, runAsyncThreads, err := GetCommitHooksWithRegistry(ctx, nameSuffix, dEnv, logger, vars)
		if err != nil {
			return nil, nil, err
		}
		dEnv.DoltDB(ctx).PrependCommitHooks(ctx, postCommitHooks...)

		outputDbs[i], err = applyReadReplicationConfigToDatabaseWithRegistry(ctx, dEnv, db, vars)
		if err != nil {
			return nil, nil, err
		}

		asyncRunners[i] = runAsyncThreads
	}
	runAsyncThreads := func(bThreads BackgroundThreads, ctxF func(context.Context) (*sql.Context, error)) error {
		var err error
		for _, f := range asyncRunners {
			if f != nil {
				err = errors.Join(err, f(bThreads, ctxF))
			}
		}
		return err
	}
	return outputDbs, runAsyncThreads, nil
}
