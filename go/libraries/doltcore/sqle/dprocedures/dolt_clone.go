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
	"path"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
)

// doltClone is the stored procedure version for the CLI command `dolt clone`.
func doltClone(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	ap := cli.CreateCloneArgParser()
	apr, err := ap.Parse(args)
	if err != nil {
		return nil, err
	}

	remoteName := apr.GetValueOrDefault(cli.RemoteParam, "origin")
	branch := apr.GetValueOrDefault(cli.BranchParam, "")
	dir, urlStr, err := getDirectoryAndUrlString(apr)
	if err != nil {
		return nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	_, remoteUrl, err := env.GetAbsRemoteUrl(sess.Provider().FileSystem(), emptyConfig(), urlStr)
	if err != nil {
		return nil, errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}

	// There are several remote params (AWS/GCP/OCI paths, creds, etc) which are pulled from the global server using
	// server config, environment vars and such. The --user flag is the only one that we can override with a command flag.
	remoteParms := map[string]string{}
	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		remoteParms[dbfactory.GRPCUsernameAuthParam] = user
	}

	depth, ok := apr.GetInt(cli.DepthFlag)
	if !ok {
		depth = -1
	}

	err = sess.Provider().CloneDatabaseFromRemote(ctx, dir, branch, remoteName, remoteUrl, depth, remoteParms)
	if err != nil {
		return nil, err
	}

	return rowToIter(int64(0)), nil
}

func emptyConfig() config.ReadableConfig {
	return &config.MapConfig{}
}

func getDirectoryAndUrlString(apr *argparser.ArgParseResults) (string, string, error) {
	if apr.NArg() < 1 || apr.NArg() > 2 {
		return "", "", errhand.BuildDError("error: invalid number of arguments: database URL must be specified and database name is optional").Build()
	}

	urlStr := apr.Arg(0)
	_, err := earl.Parse(urlStr)
	if err != nil {
		return "", "", errhand.BuildDError("error: invalid remote url: %s", urlStr).Build()
	}

	var dir string
	if apr.NArg() == 2 {
		dir = apr.Arg(1)
	} else {
		dir = path.Base(urlStr)
		if dir == "." {
			dir = path.Dir(urlStr)
		} else if dir == "/" {
			return "", "", errhand.BuildDError("Could not infer repo name. Please explicitly define a directory for this url").Build()
		}
	}

	return dir, urlStr, nil
}
