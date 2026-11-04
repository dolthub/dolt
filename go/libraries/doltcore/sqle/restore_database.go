// Copyright 2026 Dolthub, Inc.
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/types"
)

// restoreStagingDirectoryName is the directory under the data root where a restore builds the
// incoming copy of a database before swapping it into place. Database discovery only inspects the
// top level of the data directory, so nothing inside this directory is ever served, even after a
// crash. Anything left in it belongs to a restore that did not finish and can be deleted freely.
const restoreStagingDirectoryName = ".dolt_restore_staging"

// RestoreDatabaseFromRemote replaces the contents of the database named |dbName| with the contents
// of |srcDb| without ever serving a partially-restored database under that name.
//
// The incoming copy is fully materialized in a staging directory that database discovery never
// scans, while the existing database (if any) keeps serving. Only once the transfer is complete
// does the swap happen: the old database moves to the dropped-databases stash, and the staged
// directory is renamed into its place. An interruption at any point - including a process kill -
// leaves either the old database or the new one. Mid-transfer state is confined to the staging
// directory, which is never discovered and never blocks a retry.
//
// This replaces the previous restore sequence (DROP DATABASE, CREATE DATABASE, then a sync into
// the live directory), whose interruption left the name absent or bound to a table-less shell.
func (p *DoltDatabaseProvider) RestoreDatabaseFromRemote(ctx *sql.Context, dbName string, srcDb *doltdb.DoltDB) (err error) {
	if err = validateDBName(dbName); err != nil {
		return err
	}

	sess := dsess.DSessFromSess(ctx.Session)

	// The swap below is a transactional barrier, exactly as the DROP DATABASE + CREATE DATABASE
	// pair this path used to run was.
	var rsc doltdb.ReplicationStatusController
	if err = commitTransaction(ctx, sess, &rsc); err != nil {
		return err
	}

	// Reserve the name so no create, clone, or second restore can interleave with the swap.
	if err = p.reserveRestoringDatabase(dbName); err != nil {
		return err
	}
	defer p.releaseCreatingDatabase(dbName)

	stagingPath := filepath.Join(restoreStagingDirectoryName,
		fmt.Sprintf("%s-%d-%d", dbName, os.Getpid(), time.Now().UnixNano()))

	var stagingEnv *env.DoltEnv
	swapped := false
	defer func() {
		if swapped {
			return
		}
		// The staging directory never entered the served namespace, so cleaning up a failed (or
		// panicked) restore is simply deleting it, after closing what was opened underneath it.
		cleanupErr := env.CloseIncompleteDatabase(stagingEnv)
		if exists, _ := p.fs.Exists(stagingPath); exists {
			if derr := p.fs.Delete(stagingPath, true /* force / recursive */); derr != nil {
				cleanupErr = errors.Join(cleanupErr,
					fmt.Errorf("unable to clean up restore staging directory '%s': %w", stagingPath, derr))
			}
		}
		if r := recover(); r != nil {
			panic(r)
		}
		err = errors.Join(err, cleanupErr)
	}()

	stagingEnv, err = p.stageRestoredDatabase(ctx, stagingPath, srcDb, sess.Username(), sess.Email())
	if err != nil {
		return err
	}

	// The staged directory now holds a complete database. Close it and evict its cache entries so
	// the directory can be renamed with nothing open underneath it, then clear the in-progress
	// marker. The marker must be gone before the rename: an interruption between the two renames
	// below must never install a marked directory under the live name, which discovery would hide
	// and creation would refuse.
	if err = env.CloseIncompleteDatabase(stagingEnv); err != nil {
		return err
	}
	stagingFs, err := p.fs.WithWorkingDir(stagingPath)
	if err != nil {
		return err
	}
	if err = dbfactory.ClearDatabaseInProgress(stagingFs); err != nil {
		return err
	}

	// Move the database being replaced to the dropped-databases stash. DropDatabase invalidates
	// session state, runs the drop hooks, closes the database, and moves its directory aside -
	// exactly what the drop half of a forced restore did before, and what the swap needs: a free
	// name whose old contents remain recoverable.
	p.mu.RLock()
	_, isLive := p.databases[formatDbMapKeyName(dbName)]
	p.mu.RUnlock()
	if isLive {
		if err = p.DropDatabase(ctx, dbName); err != nil {
			return err
		}
	}

	err = func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// The name reservation blocks concurrent creates, so nothing can have taken the name since
		// the drop. Refuse loudly rather than overwrite if something unexpected occupies it anyway.
		if exists, _ := p.fs.Exists(dbName); exists {
			return fmt.Errorf("cannot finish restore of database '%s': its directory is unexpectedly occupied", dbName)
		}
		if err := p.fs.MoveDir(stagingPath, dbName); err != nil {
			return fmt.Errorf("unable to move restored database '%s' into place "+
				"(the previous contents are in the dropped-databases stash; use dolt_undrop('%s') to recover them): %w",
				dbName, dbName, err)
		}
		swapped = true

		newFs, err := p.fs.WithWorkingDir(dbName)
		if err != nil {
			return err
		}
		// Use LoadWithoutDB so db-load params are applied before any DB is opened, as undrop does.
		newEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, newFs, p.dbFactoryUrl, "TODO")
		p.applyDBLoadParamsToEnv(newEnv)
		return p.registerNewDatabase(ctx, dbName, newEnv)
	}()
	if err != nil {
		return err
	}

	// The transaction started at the top of this function snapshotted the databases before the
	// swap. Commit it and start a fresh one, outside the provider lock, so the caller's session
	// resumes on the restored contents rather than on roots that no longer resolve.
	return commitTransaction(ctx, sess, &rsc)
}

// stageRestoredDatabase initializes a fresh database in |stagingPath| and copies the full contents
// of |srcDb| into it: all branches, tags, working sets, and remote tracking refs. This mirrors
// what dolt_backup restore built directly under the live name before. The directory carries the
// in-progress marker for the duration, so even a scan that reached it would skip it.
func (p *DoltDatabaseProvider) stageRestoredDatabase(ctx *sql.Context, stagingPath string, srcDb *doltdb.DoltDB, username, email string) (*env.DoltEnv, error) {
	if err := p.fs.MkDirs(stagingPath); err != nil {
		return nil, err
	}
	stagingFs, err := p.fs.WithWorkingDir(stagingPath)
	if err != nil {
		return nil, err
	}
	if err = dbfactory.MarkDatabaseInProgress(stagingFs); err != nil {
		return nil, err
	}

	// Use LoadWithoutDB so we can apply db-load params before any DB is opened.
	stagingEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, stagingFs, p.dbFactoryUrl, "TODO")
	p.applyDBLoadParamsToEnv(stagingEnv)

	if err = stagingEnv.InitRepo(ctx, types.Format_DOLT, username, email, p.defaultBranch); err != nil {
		return stagingEnv, err
	}

	tmpDir, err := stagingEnv.TempTableFilesDir()
	if err != nil {
		return stagingEnv, err
	}

	// Unlike a clone, a restore copies everything, including local branches and working sets.
	pull.WithDiscardingStatsCh(func(statsCh chan pull.Stats) {
		err = actions.SyncRoots(ctx, srcDb, stagingEnv.DoltDB(ctx), tmpDir, actions.SyncRootsDBRelationshipUnrelated, statsCh)
	})
	return stagingEnv, err
}

// reserveRestoringDatabase records |dbName| in p.creatingDatabases while a restore replaces its
// contents. Unlike reserveCreatingDatabase it tolerates a live database under the name - that is
// the database being replaced - but refuses when another create, clone, restore, or drop of the
// name is already in flight. Release with releaseCreatingDatabase.
func (p *DoltDatabaseProvider) reserveRestoringDatabase(dbName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := formatDbMapKeyName(dbName)
	if _, ok := p.deletingDatabases[key]; ok {
		return fmt.Errorf("cannot restore database '%s': it is being dropped by a concurrent operation", dbName)
	}
	if _, ok := p.creatingDatabases[key]; ok {
		return fmt.Errorf("cannot restore database '%s': a concurrent operation is creating or restoring it", dbName)
	}
	p.creatingDatabases[key] = struct{}{}
	return nil
}
