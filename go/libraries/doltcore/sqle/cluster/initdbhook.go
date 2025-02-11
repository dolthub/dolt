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

package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
)

func NewInitDatabaseHook(controller *Controller, bt *sql.BackgroundThreads) sqle.InitDatabaseHook {
	return func(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv, db dsess.SqlDatabase) error {
		dialprovider := controller.gRPCDialProvider(denv)
		var remoteDBs []func(context.Context) (*doltdb.DoltDB, error)
		var remoteUrls []string
		for _, r := range controller.cfg.StandbyRemotes() {
			// TODO: url sanitize name
			remoteUrl := strings.Replace(r.RemoteURLTemplate(), dsess.URLTemplateDatabasePlaceholder, name, -1)

			// We're going to check if this database already has
			// the remote we're trying to add. This can happen in
			// the DOLT_UNDROP case, for example. If a matching
			// remote exists, we assert that it has the expected
			// URL. It's an error otherwise.
			remotes, err := denv.GetRemotes()
			if err != nil {
				// XXX: An error here means we are not replicating.
				return err
			}

			var er env.Remote
			var ok bool
			if er, ok = remotes.Get(r.Name()); ok {
				if er.Url != remoteUrl {
					return fmt.Errorf("invalid remote (%s) for cluster replication found in database %s: expect url %s but the existing remote had url %s", r.Name(), name, remoteUrl, er.Url)
				}
			} else {
				// TODO: Assert remotesapi URL.
				er = env.NewRemote(r.Name(), remoteUrl, nil)
				err := denv.AddRemote(er)
				if err != nil {
					return err
				}
			}

			remoteDBs = append(remoteDBs, func(ctx context.Context) (*doltdb.DoltDB, error) {
				return er.GetRemoteDB(ctx, types.Format_Default, dialprovider)
			})
			remoteUrls = append(remoteUrls, remoteUrl)
		}

		// When we create a new database, we stop trying to replicate a
		// previous drop of that database to the replicas. Successfully
		// replicating a new head update will set the state of any
		// existing database to the state of this new database going
		// forward.
		controller.cancelDropDatabaseReplication(name)

		role, _ := controller.roleAndEpoch()
		for i, r := range controller.cfg.StandbyRemotes() {
			ttfdir, err := denv.TempTableFilesDir()
			if err != nil {
				// XXX: An error here means we are not replicating to every standby.
				return err
			}
			commitHook := newCommitHook(controller.lgr, r.Name(), remoteUrls[i], name, role, remoteDBs[i], denv.DoltDB(ctx), ttfdir)
			denv.DoltDB(ctx).PrependCommitHooks(ctx, commitHook)
			controller.registerCommitHook(commitHook)
			if err := commitHook.Run(bt, controller.sqlCtxFactory); err != nil {
				// XXX: An error here means we are not replicating to every standby.
				return err
			}
		}

		return nil
	}
}
