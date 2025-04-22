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

package dprocedures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	cmdFailure = 1
	cmdSuccess = 0
)

func init() {
	if os.Getenv(dconfig.EnvDisableGcProcedure) != "" {
		DoltGCFeatureFlag = false
	}
	if choice := os.Getenv(dconfig.EnvGCSafepointControllerChoice); choice != "" {
		if choice == "session_aware" {
			UseSessionAwareSafepointController = true
		} else if choice != "kill_connections" {
			panic("Invalid value for " + dconfig.EnvGCSafepointControllerChoice + ". must be session_aware or kill_connections")
		}
	}
}

var DoltGCFeatureFlag = true
var UseSessionAwareSafepointController = false

// doltGC is the stored procedure to run online garbage collection on a database.
func doltGC(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	if !DoltGCFeatureFlag {
		return nil, errors.New("DOLT_GC() stored procedure disabled")
	}
	res, err := doDoltGC(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

var ErrServerPerformedGC = errors.New("this connection was established when this server performed an online garbage collection. this connection can no longer be used. please reconnect.")

// The original behavior safepoint controller, which kills all connections right as the GC process is being finalized.
// The only connection which is left up is the connection on which dolt_gc is called, but that connection is
// invalidated in such a way that all future queries on it return an error.
type killConnectionsSafepointController struct {
	callCtx   *sql.Context
	doltDB    *doltdb.DoltDB
	origEpoch int
}

func (sc killConnectionsSafepointController) BeginGC(ctx context.Context, keeper func(hash.Hash) bool) error {
	sc.doltDB.PurgeCaches()
	return nil
}

func (sc killConnectionsSafepointController) EstablishPreFinalizeSafepoint(ctx context.Context) error {
	return nil
}

func checkEpochSame(origEpoch int) error {
	// Here we need to sanity check role and epoch.
	if origEpoch != -1 {
		if _, role, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleVariable); ok {
			if role.(string) != "primary" {
				return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but now our role is %s", role.(string))
			}
			_, epoch, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleEpochVariable)
			if !ok {
				return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but we can no longer read the cluster role epoch.")
			}
			if origEpoch != epoch.(int) {
				return fmt.Errorf("dolt_gc failed: when we began we were primary in the cluster at epoch %d, but now we are at epoch %d. for gc to safely finalize, our role and epoch must not change throughout the gc.", origEpoch, epoch.(int))
			}
		} else {
			return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but we can no longer read the cluster role.")
		}
	}
	return nil
}

func (sc killConnectionsSafepointController) EstablishPostFinalizeSafepoint(ctx context.Context) error {
	err := checkEpochSame(sc.origEpoch)
	if err != nil {
		return err
	}

	killed := make(map[uint32]struct{})
	processes := sc.callCtx.ProcessList.Processes()
	for _, p := range processes {
		if p.Connection != sc.callCtx.Session.ID() {
			// Kill any inflight query.
			sc.callCtx.ProcessList.Kill(p.Connection)
			// Tear down the connection itself.
			sc.callCtx.KillConnection(p.Connection)
			killed[p.Connection] = struct{}{}
		}
	}

	// Look in processes until the connections are actually gone.
	params := backoff.NewExponentialBackOff()
	params.InitialInterval = 1 * time.Millisecond
	params.MaxInterval = 25 * time.Millisecond
	params.MaxElapsedTime = 10 * time.Second
	var unkilled map[uint32]struct{}
	err = backoff.Retry(func() error {
		unkilled = make(map[uint32]struct{})
		processes := sc.callCtx.ProcessList.Processes()
		for _, p := range processes {
			if _, ok := killed[p.Connection]; ok {
				unkilled[p.Connection] = struct{}{}
				sc.callCtx.ProcessList.Kill(p.Connection)
			}
		}
		if len(unkilled) > 0 {
			return errors.New("could not establish safepont")
		}
		return nil
	}, params)
	if err != nil {
		return fmt.Errorf("%w: still saw these connections in the process list: %v", err, unkilled)
	}
	sc.callCtx.Session.SetTransaction(nil)
	dsess.DSessFromSess(sc.callCtx.Session).SetValidateErr(ErrServerPerformedGC)
	return nil
}

func (sc killConnectionsSafepointController) CancelSafepoint() {
}

type sessionAwareSafepointController struct {
	controller  *gcctx.GCSafepointController
	dbname      string
	callSession *dsess.DoltSession
	origEpoch   int
	doltDB      *doltdb.DoltDB

	waiter *gcctx.GCSafepointWaiter
	keeper func(hash.Hash) bool
}

func (sc *sessionAwareSafepointController) visit(ctx context.Context, sess gcctx.GCRootsProvider) error {
	return sess.VisitGCRoots(ctx, sc.dbname, sc.keeper)
}

func (sc *sessionAwareSafepointController) BeginGC(ctx context.Context, keeper func(hash.Hash) bool) error {
	sc.doltDB.PurgeCaches()
	sc.keeper = keeper
	err := sc.visit(ctx, sc.callSession)
	if err != nil {
		return err
	}
	sc.waiter = sc.controller.Waiter(ctx, sc.callSession, sc.visit)
	return nil
}

func (sc *sessionAwareSafepointController) EstablishPreFinalizeSafepoint(ctx context.Context) error {
	return sc.waiter.Wait(ctx)
}

func (sc *sessionAwareSafepointController) EstablishPostFinalizeSafepoint(ctx context.Context) error {
	return nil
}

func (sc *sessionAwareSafepointController) CancelSafepoint() {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	sc.waiter.Wait(canceledCtx)
}

func doDoltGC(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return cmdFailure, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return cmdFailure, err
	}

	apr, err := cli.CreateGCArgParser().Parse(args)
	if err != nil {
		return cmdFailure, err
	}

	if apr.NArg() != 0 {
		return cmdFailure, InvalidArgErr
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	ddb, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return cmdFailure, fmt.Errorf("Could not load database %s", dbName)
	}

	if apr.Contains(cli.ShallowFlag) && apr.Contains(cli.FullFlag) {
		return cmdFailure, fmt.Errorf("cannot supply both --shallow and --full to dolt_gc: %w", InvalidArgErr)
	}

	if apr.Contains(cli.ShallowFlag) {
		err = ddb.ShallowGC(ctx)
		if err != nil {
			return cmdFailure, err
		}
	} else {
		mode := types.GCModeDefault
		if apr.Contains(cli.FullFlag) {
			mode = types.GCModeFull
		}

		cmpLvl := chunks.NoArchive
		if apr.Contains(cli.ArchiveLevelParam) {
			lvl, ok := apr.GetInt(cli.ArchiveLevelParam)
			if !ok {
				return cmdFailure, fmt.Errorf("parse error for value for %s: %s", cli.ArchiveLevelParam, apr.GetValues()[cli.ArchiveLevelParam])
			}
			if lvl < int(chunks.NoArchive) || lvl > int(chunks.MaxArchiveLevel) {
				return cmdFailure, fmt.Errorf("invalid value for %s: %d", cli.ArchiveLevelParam, lvl)
			}
			cmpLvl = chunks.GCArchiveLevel(lvl)
		}

		err := RunDoltGC(ctx, ddb, mode, cmpLvl, ctx.GetCurrentDatabase())
		if err != nil {
			return cmdFailure, err
		}
	}

	return cmdSuccess, nil
}

func RunDoltGC(ctx *sql.Context, ddb *doltdb.DoltDB, mode types.GCMode, cmp chunks.GCArchiveLevel, dbname string) error {
	var sc types.GCSafepointController
	if UseSessionAwareSafepointController {
		dSess := dsess.DSessFromSess(ctx.Session)
		gcSafepointController := dSess.GCSafepointController()
		sc = &sessionAwareSafepointController{
			callSession: dSess,
			dbname:      dbname,
			controller:  gcSafepointController,
			doltDB:      ddb,
		}
		_, err := statsStop(ctx)
		if err != nil {
			ctx.GetLogger().Infof("gc stats interrupt failed: %s", err.Error())
		}
		defer func() {
			_, err = statsRestart(ctx)
			if err != nil {
				ctx.GetLogger().Infof("gc stats restart failed: %s", err.Error())
			}
		}()
	} else {
		// Legacy safepoint controller behavior was to not
		// allow GC on a standby server. GC on a standby server
		// with killConnections safepoints should be safe now,
		// but we retain this legacy behavior for now.
		origepoch := -1
		if _, role, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleVariable); ok {
			// TODO: magic constant...
			if role.(string) != "primary" {
				return fmt.Errorf("cannot run a full dolt_gc() while cluster replication is enabled and role is %s; must be the primary", role.(string))
			}
			_, epoch, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleEpochVariable)
			if !ok {
				return fmt.Errorf("internal error: cannot run a full dolt_gc(); cluster replication is enabled but could not read %s", dsess.DoltClusterRoleEpochVariable)
			}
			origepoch = epoch.(int)
		}
		sc = killConnectionsSafepointController{
			origEpoch: origepoch,
			callCtx:   ctx,
			doltDB:    ddb,
		}
	}
	return ddb.GC(ctx, mode, cmp, sc)
}
