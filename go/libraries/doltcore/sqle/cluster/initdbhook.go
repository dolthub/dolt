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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
)

func NewInitDatabaseHook(controller *Controller, bt *sql.BackgroundThreads, orig sqle.InitDatabaseHook) sqle.InitDatabaseHook {
	if controller == nil {
		return orig
	}
	return func(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, name string, denv *env.DoltEnv) error {
		var err error
		err = orig(ctx, pro, name, denv)
		if err != nil {
			return err
		}

		dialprovider := controller.gRPCDialProvider(denv)
		var remoteDBs []func(context.Context) (*doltdb.DoltDB, error)
		var remoteUrls []string
		for _, r := range controller.cfg.StandbyRemotes() {
			// TODO: url sanitize name
			remoteUrl := strings.Replace(r.RemoteURLTemplate(), dsess.URLTemplateDatabasePlaceholder, name, -1)

			// TODO: Assert remotesapi URL.
			r := env.NewRemote(r.Name(), remoteUrl, nil)

			err := denv.AddRemote(r)
			if err != nil {
				return err
			}

			remoteDBs = append(remoteDBs, func(ctx context.Context) (*doltdb.DoltDB, error) {
				return r.GetRemoteDB(ctx, types.Format_Default, dialprovider)
			})
			remoteUrls = append(remoteUrls, remoteUrl)
		}

		role, _ := controller.roleAndEpoch()
		for i, r := range controller.cfg.StandbyRemotes() {
			ttfdir, err := denv.TempTableFilesDir()
			if err != nil {
				return err
			}
			commitHook := newCommitHook(controller.lgr, r.Name(), remoteUrls[i], name, role, remoteDBs[i], denv.DoltDB, ttfdir)
			denv.DoltDB.PrependCommitHook(ctx, commitHook)
			controller.registerCommitHook(commitHook)
			if err := commitHook.Run(bt); err != nil {
				return err
			}
		}

		return nil
	}
}
