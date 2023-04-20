// Copyright 2019 Dolthub, Inc.
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

package commands

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"path/filepath"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
)

var fwtStageName = "fwt"

func GetWorkingWithVErr(dEnv *env.DoltEnv) (*doltdb.RootValue, errhand.VerboseError) {
	working, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get working.").AddCause(err).Build()
	}

	return working, nil
}

func GetStagedWithVErr(dEnv *env.DoltEnv) (*doltdb.RootValue, errhand.VerboseError) {
	staged, err := dEnv.StagedRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	return staged, nil
}

func UpdateWorkingWithVErr(dEnv *env.DoltEnv, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	err := dEnv.UpdateWorkingRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the working root state").Build()
	}

	return nil
}

func MaybeGetCommitWithVErr(dEnv *env.DoltEnv, maybeCommit string) (*doltdb.Commit, errhand.VerboseError) {
	cm, err := actions.MaybeGetCommit(context.TODO(), dEnv, maybeCommit)

	if err != nil {
		bdr := errhand.BuildDError("fatal: Unable to read from data repository.")
		return nil, bdr.AddCause(err).Build()
	}

	return cm, nil
}

// BuildSqlEngineQueryist Utility function to build a local SQLEngine for use interacting with data on disk using
// SQL queries.
func BuildSqlEngineQueryist(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (cli.LateBindQueryist, errhand.VerboseError) {
	// Retrieve username and password from command line, if provided
	username := DefaultUser
	if user, ok := apr.GetValue(UserFlag); ok {
		username = user
	}

	// data-dir args come either from the global args or the subcommand args.  We need to check both.
	var dataDir string
	dataDirGiven := false
	if dataDirPath, ok := apr.GetValue(DataDirFlag); ok {
		dataDir = dataDirPath
		dataDirGiven = true
	}

	mrEnv, dataDir, verr := getMultiRepoEnv(ctx, dataDir, dEnv)
	if verr != nil {
		return nil, verr
	}

	// need to return cfgdirpath and error
	var cfgDirPath string
	cfgDir, cfgDirSpecified := apr.GetValue(CfgDirFlag)
	if cfgDirSpecified {
		cfgDirPath = cfgDir
	} else if dataDirGiven {
		cfgDirPath = filepath.Join(dataDir, DefaultCfgDirName)
	} else {
		// Look in parent directory for doltcfg
		parentDirCfg := filepath.Join("..", DefaultCfgDirName)
		parentExists, isDir := dEnv.FS.Exists(parentDirCfg)
		parentDirExists := parentExists && isDir

		// Look in data directory (which is necessarily current directory) for doltcfg
		currDirCfg := filepath.Join(dataDir, DefaultCfgDirName)
		currExists, isDir := dEnv.FS.Exists(currDirCfg)
		currDirExists := currExists && isDir

		// Error if both current and parent exist
		if currDirExists && parentDirExists {
			p1, err := dEnv.FS.Abs(cfgDirPath)
			if err != nil {
				return nil, errhand.VerboseErrorFromError(err)
			}
			p2, err := dEnv.FS.Abs(parentDirCfg)
			if err != nil {
				return nil, errhand.VerboseErrorFromError(err)
			}
			return nil, errhand.VerboseErrorFromError(ErrMultipleDoltCfgDirs.New(p1, p2))
		}

		// Assign the one that exists, defaults to current if neither exist
		if parentDirExists {
			cfgDirPath = parentDirCfg
		} else {
			cfgDirPath = currDirCfg
		}
	}

	var err error
	// If no privilege filepath specified, default to doltcfg directory
	privsFp, hasPrivsFp := apr.GetValue(PrivsFilePathFlag)
	if !hasPrivsFp {
		privsFp, err = dEnv.FS.Abs(filepath.Join(cfgDirPath, DefaultPrivsName))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	// If no branch control file path is specified, default to doltcfg directory
	branchControlFilePath, hasBCFilePath := apr.GetValue(BranchCtrlPathFlag)
	if !hasBCFilePath {
		branchControlFilePath, err = dEnv.FS.Abs(filepath.Join(cfgDirPath, DefaultBranchCtrlName))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	binder, err := newLateBindingEngine(ctx, apr, cfgDirPath, privsFp, branchControlFilePath, username, mrEnv)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return binder, nil
}

func newLateBindingEngine(
	ctx context.Context,
	apr *argparser.ArgParseResults,
	cfgDirPath string,
	privsFp string,
	branchControlFilePath string,
	username string,
	mrEnv *env.MultiRepoEnv,
) (cli.LateBindQueryist, error) {

	config := &engine.SqlEngineConfig{
		DoltCfgDirPath:     cfgDirPath,
		PrivFilePath:       privsFp,
		BranchCtrlFilePath: branchControlFilePath,
		ServerUser:         username,
		ServerHost:         "localhost", // NM4 - was a const before. shrug?
		Autocommit:         true,
	}

	var lateBinder cli.LateBindQueryist = func(ctx2 context.Context) (cli.Queryist, *sql.Context, func(), error) {
		se, err := engine.NewSqlEngine(
			ctx2,
			mrEnv,
			config,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		sqlCtx, err := se.NewDefaultContext(ctx2)
		if err != nil {
			return nil, nil, nil, err
		}

		// Whether we're running in shell mode or some other mode, sql commands from the command line always have a current
		// database set when you begin using them.
		sqlCtx.SetCurrentDatabase(mrEnv.GetFirstDatabase())

		// Add specified user as new superuser, if it doesn't already exist
		if user := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, config.ServerHost, false); user == nil {
			se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, config.ServerHost, config.ServerPass)
		}

		// Set client to specified user
		sqlCtx.Session.SetClient(sql.Client{User: config.ServerUser, Address: config.ServerHost, Capabilities: 0})
		return se, sqlCtx, func() { se.Close() }, nil

	}

	return lateBinder, nil
}
