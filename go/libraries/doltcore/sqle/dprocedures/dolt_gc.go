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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	cmdFailure = 1
	cmdSuccess = 0
)

func init() {
	if os.Getenv("DOLT_DISABLE_GC_PROCEDURE") != "" {
		DoltGCFeatureFlag = false
	}
}

var DoltGCFeatureFlag = true

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

	if apr.Contains(cli.ShallowFlag) {
		err = ddb.ShallowGC(ctx)
		if err != nil {
			return cmdFailure, err
		}
	} else {
		// Currently, if this server is involved in cluster
		// replication, a full GC is only safe to run on the primary.
		// We assert that we are the primary here before we begin, and
		// we assert again that we are the primary at the same epoch as
		// we establish the safepoint.

		origepoch := -1
		if _, role, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleVariable); ok {
			// TODO: magic constant...
			if role.(string) != "primary" {
				return cmdFailure, fmt.Errorf("cannot run a full dolt_gc() while cluster replication is enabled and role is %s; must be the primary", role.(string))
			}
			_, epoch, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleEpochVariable)
			if !ok {
				return cmdFailure, fmt.Errorf("internal error: cannot run a full dolt_gc(); cluster replication is enabled but could not read %s", dsess.DoltClusterRoleEpochVariable)
			}
			origepoch = epoch.(int)
		}

		// TODO: If we got a callback at the beginning and an
		// (allowed-to-block) callback at the end, we could more
		// gracefully tear things down.
		err = ddb.GC(ctx, func() error {
			if origepoch != -1 {
				// Here we need to sanity check role and epoch.
				if _, role, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleVariable); ok {
					if role.(string) != "primary" {
						return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but now our role is %s", role.(string))
					}
					_, epoch, ok := sql.SystemVariables.GetGlobal(dsess.DoltClusterRoleEpochVariable)
					if !ok {
						return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but we can no longer read the cluster role epoch.")
					}
					if origepoch != epoch.(int) {
						return fmt.Errorf("dolt_gc failed: when we began we were primary in the cluster at epoch %d, but now we are at epoch %d. for gc to safely finalize, our role and epoch must not change throughout the gc.", origepoch, epoch.(int))
					}
				} else {
					return fmt.Errorf("dolt_gc failed: when we began we were a primary in a cluster, but we can no longer read the cluster role.")
				}
			}

			killed := make(map[uint32]struct{})
			processes := ctx.ProcessList.Processes()
			for _, p := range processes {
				if p.Connection != ctx.Session.ID() {
					// Kill any inflight query.
					ctx.ProcessList.Kill(p.Connection)
					// Tear down the connection itself.
					ctx.KillConnection(p.Connection)
					killed[p.Connection] = struct{}{}
				}
			}

			// Look in processes until the connections are actually gone.
			params := backoff.NewExponentialBackOff()
			params.InitialInterval = 1 * time.Millisecond
			params.MaxInterval = 25 * time.Millisecond
			params.MaxElapsedTime = 3 * time.Second
			err := backoff.Retry(func() error {
				processes := ctx.ProcessList.Processes()
				for _, p := range processes {
					if _, ok := killed[p.Connection]; ok {
						return errors.New("unable to establish safepoint.")
					}
				}
				return nil
			}, params)
			if err != nil {
				return err
			}
			ctx.Session.SetTransaction(nil)
			dsess.DSessFromSess(ctx.Session).SetValidateErr(ErrServerPerformedGC)
			return nil
		})
		if err != nil {
			return cmdFailure, err
		}
	}

	return cmdSuccess, nil
}
