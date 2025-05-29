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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

// doltRemote is the stored procedure version for the CLI command `dolt remote`.
func doltRemote(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRemote(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

// doDoltRemote is used as sql dolt_remote command for only creating or deleting remotes, not listing.
// To list remotes, dolt_remotes system table is used.
func doDoltRemote(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateRemoteArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	if apr.NArg() == 0 {
		return 1, fmt.Errorf("error: invalid argument, use 'dolt_remotes' system table to list remotes")
	}

	var rsc doltdb.ReplicationStatusController

	switch apr.Arg(0) {
	case "add":
		err = addRemote(ctx, dbName, dbData, apr, dSess)
	case "remove", "rm":
		err = removeRemote(ctx, dbData, apr, &rsc)
	default:
		err = fmt.Errorf("error: invalid argument")
	}

	if err != nil {
		return 1, err
	}

	dsess.WaitForReplicationController(ctx, rsc)

	return 0, nil
}

func addRemote(_ *sql.Context, dbName string, dbd env.DbData[*sql.Context], apr *argparser.ArgParseResults, sess *dsess.DoltSession) error {
	if apr.NArg() != 3 {
		return fmt.Errorf("error: invalid argument")
	}

	remoteName := strings.TrimSpace(apr.Arg(1))
	remoteUrl := apr.Arg(2)

	dbFs, err := sess.Provider().FileSystemForDatabase(dbName)
	if err != nil {
		return err
	}

	_, absRemoteUrl, err := env.GetAbsRemoteUrl(dbFs, &config.MapConfig{}, remoteUrl)
	if err != nil {
		return err
	}

	r := env.NewRemote(remoteName, absRemoteUrl, map[string]string{})
	return dbd.Rsw.AddRemote(r)
}

func removeRemote(ctx *sql.Context, dbd env.DbData[*sql.Context], apr *argparser.ArgParseResults, rsc *doltdb.ReplicationStatusController) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("error: invalid argument")
	}

	old := strings.TrimSpace(apr.Arg(1))

	remotes, err := dbd.Rsr.GetRemotes()
	if err != nil {
		return err
	}

	remote, ok := remotes.Get(old)
	if !ok {
		return fmt.Errorf("error: unknown remote: '%s'", old)
	}

	ddb := dbd.Ddb
	refs, err := ddb.GetRemoteRefs(ctx)
	if err != nil {
		return fmt.Errorf("error: %w, cause: %s", env.ErrFailedToReadFromDb, err.Error())
	}

	for _, r := range refs {
		rr := r.(ref.RemoteRef)

		if rr.GetRemote() == remote.Name {
			err = ddb.DeleteBranch(ctx, rr, rsc)

			if err != nil {
				return fmt.Errorf("%w; failed to delete remote tracking ref '%s'; %s", env.ErrFailedToDeleteRemote, rr.String(), err.Error())
			}
		}
	}

	// Remove branch configurations that reference the removed remote
	branches, err := dbd.Rsr.GetBranches()
	if err != nil {
		return fmt.Errorf("failed to get branches: %w", err)
	}

	var branchesToUpdate []string
	branches.Iter(func(branchName string, config env.BranchConfig) bool {
		if config.Remote == remote.Name {
			branchesToUpdate = append(branchesToUpdate, branchName)
		}
		return true
	})

	// Clear the remote tracking for these branches by updating their configs
	for _, branchName := range branchesToUpdate {
		currentConfig, _ := branches.Get(branchName)
		updatedConfig := env.BranchConfig{
			Merge:  currentConfig.Merge,
			Remote: "", // Clear the remote reference
		}
		err = dbd.Rsw.UpdateBranch(branchName, updatedConfig)
		if err != nil {
			return fmt.Errorf("failed to update branch config for %s: %w", branchName, err)
		}
	}

	return dbd.Rsw.RemoveRemote(ctx, remote.Name)
}
