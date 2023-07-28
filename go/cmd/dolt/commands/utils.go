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
	"crypto/sha1"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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

// NewArgFreeCliContext creates a new CliContext instance with no arguments using a local SqlEngine. This is useful for testing primarily
func NewArgFreeCliContext(ctx context.Context, dEnv *env.DoltEnv) (cli.CliContext, errhand.VerboseError) {
	mrEnv, err := env.MultiEnvForSingleEnv(ctx, dEnv)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	emptyArgs := argparser.NewEmptyResults()
	emptyArgs, creds, _ := cli.BuildUserPasswordPrompt(emptyArgs)
	lateBind, verr := BuildSqlEngineQueryist(ctx, dEnv.FS, mrEnv, creds, emptyArgs)

	if err != nil {
		return nil, verr
	}
	return cli.NewCliContext(argparser.NewEmptyResults(), dEnv.Config, lateBind)
}

// BuildSqlEngineQueryist Utility function to build a local SQLEngine for use interacting with data on disk using
// SQL queries. ctx, cwdFS, mrEnv, and apr must all be non-nil.
func BuildSqlEngineQueryist(ctx context.Context, cwdFS filesys.Filesys, mrEnv *env.MultiRepoEnv, creds *cli.UserPassword, apr *argparser.ArgParseResults) (cli.LateBindQueryist, errhand.VerboseError) {
	if ctx == nil || cwdFS == nil || mrEnv == nil || creds == nil || apr == nil {
		errhand.VerboseErrorFromError(fmt.Errorf("Invariant violated. Nil argument provided to BuildSqlEngineQueryist"))
	}

	// We want to know if the user provided us the data-dir flag, but we want to use the abs value used to
	// create the DoltEnv. This is a little messy.
	dataDir, dataDirGiven := apr.GetValue(DataDirFlag)
	dataDir, err := cwdFS.Abs(dataDir)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	// need to return cfgdirpath and error
	var cfgDirPath string
	cfgDir, cfgDirSpecified := apr.GetValue(CfgDirFlag)
	if cfgDirSpecified {
		cfgDirPath, err = cwdFS.Abs(cfgDir)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	} else if dataDirGiven {
		cfgDirPath = filepath.Join(dataDir, DefaultCfgDirName)
	} else {
		// Look in CWD parent directory for doltcfg
		parentDirCfg := filepath.Join("..", DefaultCfgDirName)
		parentExists, isDir := cwdFS.Exists(parentDirCfg)
		parentDirExists := parentExists && isDir

		// Look in data directory for doltcfg
		dataDirCfg := filepath.Join(dataDir, DefaultCfgDirName)
		dataDirCfgExists, isDir := cwdFS.Exists(dataDirCfg)
		currDirExists := dataDirCfgExists && isDir

		// Error if both CWD/../.doltfcfg and dataDir/.doltcfg exist because it's unclear which to use.
		if currDirExists && parentDirExists {
			p1, err := cwdFS.Abs(cfgDirPath)
			if err != nil {
				return nil, errhand.VerboseErrorFromError(err)
			}
			p2, err := cwdFS.Abs(parentDirCfg)
			if err != nil {
				return nil, errhand.VerboseErrorFromError(err)
			}
			return nil, errhand.VerboseErrorFromError(ErrMultipleDoltCfgDirs.New(p1, p2))
		}

		// Assign the one that exists, defaults to current if neither exist
		if parentDirExists {
			cfgDirPath = parentDirCfg
		} else {
			cfgDirPath = dataDirCfg
		}
	}

	// If no privilege filepath specified, default to doltcfg directory
	privsFp, hasPrivsFp := apr.GetValue(PrivsFilePathFlag)
	if !hasPrivsFp {
		privsFp, err = cwdFS.Abs(filepath.Join(cfgDirPath, DefaultPrivsName))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	} else {
		privsFp, err = cwdFS.Abs(privsFp)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	// If no branch control file path is specified, default to doltcfg directory
	branchControlFilePath, hasBCFilePath := apr.GetValue(BranchCtrlPathFlag)
	if !hasBCFilePath {
		branchControlFilePath, err = cwdFS.Abs(filepath.Join(cfgDirPath, DefaultBranchCtrlName))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	} else {
		branchControlFilePath, err = cwdFS.Abs(branchControlFilePath)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	// Whether we're running in shell mode or some other mode, sql commands from the command line always have a current
	// database set when you begin using them.
	database, hasDB := apr.GetValue(UseDbFlag)
	if !hasDB {
		database = mrEnv.GetFirstDatabase()
	}

	binder, err := newLateBindingEngine(cfgDirPath, privsFp, branchControlFilePath, creds, database, mrEnv)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return binder, nil
}

func newLateBindingEngine(
	cfgDirPath string,
	privsFp string,
	branchControlFilePath string,
	creds *cli.UserPassword,
	database string,
	mrEnv *env.MultiRepoEnv,
) (cli.LateBindQueryist, error) {

	config := &engine.SqlEngineConfig{
		DoltCfgDirPath:     cfgDirPath,
		PrivFilePath:       privsFp,
		BranchCtrlFilePath: branchControlFilePath,
		ServerUser:         creds.Username,
		ServerPass:         creds.Password,
		ServerHost:         "localhost",
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
		sqlCtx.SetCurrentDatabase(database)

		rawDb := se.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
		salt, err := rawDb.Salt()
		if err != nil {
			return nil, nil, nil, err
		}

		var dbUser string
		if creds.Specified {
			dbUser = creds.Username

			// When running in local mode, we want to attempt respect the user/pwd they provided. If they didn't provide
			// one, we'll give then super user privs. Respecting the user/pwd is not a security stance - it's there
			// to enable testing of application settings.

			authResponse := buildAuthResponse(salt, config.ServerPass)

			err := passwordValidate(rawDb, salt, dbUser, authResponse)
			if err != nil {
				return nil, nil, nil, err
			}

		} else {
			dbUser = DefaultUser
			ed := rawDb.Editor()
			user := rawDb.GetUser(ed, dbUser, config.ServerHost, false)
			ed.Close()
			if user != nil {
				// Want to ensure that the user has an empty password. If it has a password, we'll error
				err := passwordValidate(rawDb, salt, dbUser, nil)
				if err != nil {
					return nil, nil, nil, err
				}
			}

			// If the user doesn't exist, we'll create it with superuser privs.
			ed = rawDb.Editor()
			defer ed.Close()
			rawDb.AddSuperUser(ed, dbUser, config.ServerHost, "")
		}

		// Set client to specified user
		sqlCtx.Session.SetClient(sql.Client{User: dbUser, Address: config.ServerHost, Capabilities: 0})
		return se, sqlCtx, func() { se.Close() }, nil
	}

	return lateBinder, nil
}

func GetRowsForSql(queryist cli.Queryist, sqlCtx *sql.Context, query string) ([]sql.Row, error) {
	schema, rowIter, err := queryist.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, schema, rowIter)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// InterpolateAndRunQuery interpolates a query, executes it, and returns the result rows.
// Since this method does not return a schema, this method should be used only for fire-and-forget types of queries.
func InterpolateAndRunQuery(queryist cli.Queryist, sqlCtx *sql.Context, queryTemplate string, params ...interface{}) ([]sql.Row, error) {
	query, err := dbr.InterpolateForDialect(queryTemplate, params, dialect.MySQL)
	if err != nil {
		return nil, fmt.Errorf("error interpolating query: %w", err)
	}
	return GetRowsForSql(queryist, sqlCtx, query)
}

// GetTinyIntColAsBool returns the value of a tinyint column as a bool
// This is necessary because Queryist may return a tinyint column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func GetTinyIntColAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return v, nil
	case int:
		return v == 1, nil
	case string:
		return v == "1", nil
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool, int, or string", v)
	}
}

// getInt64ColAsInt64 returns the value of an int64 column as a string
// This is necessary because Queryist may return an int64 column as an int64 (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func getInt64ColAsInt64(col interface{}) (int64, error) {
	switch v := col.(type) {
	case int:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		iv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return iv, nil
	default:
		return 0, fmt.Errorf("unexpected type %T, was expecting int64, uint64 or string", v)
	}
}

func getActiveBranchName(sqlCtx *sql.Context, queryEngine cli.Queryist) (string, error) {
	query := "SELECT active_branch()"
	rows, err := GetRowsForSql(queryEngine, sqlCtx, query)
	if err != nil {
		return "", err
	}

	if len(rows) != 1 {
		return "", fmt.Errorf("unexpectedly received multiple rows in '%s': %s", query, rows)
	}
	row := rows[0]
	if len(row) != 1 {
		return "", fmt.Errorf("unexpectedly received multiple columns in '%s': %s", query, row)
	}
	branchName, ok := row[0].(string)
	if !ok {
		return "", fmt.Errorf("unexpectedly received non-string column in '%s': %s", query, row[0])
	}
	return branchName, nil
}

func getTimestampColAsUint64(col interface{}) (uint64, error) {
	switch v := col.(type) {
	case string:
		t, err := time.Parse("2006-01-02 15:04:05.999", v)
		if err != nil {
			return 0, fmt.Errorf("error parsing timestamp %s: %w", v, err)
		}
		return uint64(t.UnixMilli()), nil
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case time.Time:
		return uint64(v.UnixMilli()), nil
	default:
		return 0, fmt.Errorf("unexpected type %T, was expecting int64, uint64 or time.Time", v)
	}
}

// passwordValidate validates the password for the given user. This is a helper function around ValidateHash. Returns
// nil if the user is authenticated, an error otherwise.
func passwordValidate(rawDb *mysql_db.MySQLDb, salt []byte, user string, authResponse []byte) error {
	// The port is meaningless here. It's going to be stripped in the ValidateHash function
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:3306")

	authenticated, err := rawDb.ValidateHash(salt, user, authResponse, addr)
	if err != nil {
		return err
	}
	if authenticated == nil {
		// Shouldn't happen - err above should happen instead. But just in case...
		return fmt.Errorf("unable to authenticate user %s", user)
	}
	return nil
}

// buildAuthResponse takes the user password and server salt to construct an authResponse. This is the client
// side logic of the mysql_native_password authentication protocol.
func buildAuthResponse(salt []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// Final goal is to get to this:
	// XOR(SHA(password), SHA(salt, SHA(SHA(password))))

	crypt := sha1.New()
	crypt.Write([]byte(password))
	shaPwd := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(shaPwd)
	// This is the value stored in the Database in the mysql.user table in the authentication_string column when
	// the plugin is set to mysql_native_password.
	shaShaPwd := crypt.Sum(nil)

	// Using salt and shaShaPwd (both of which the server knows) we execute an XOR with shaPwd. This means the server
	// can XOR the result of this with shaShaPwd to get shaPwd. Then the server takes the sha of that value and validates
	// it's what it has stored in the database.
	crypt.Reset()
	crypt.Write(salt)
	crypt.Write(shaShaPwd)
	scramble := crypt.Sum(nil)
	for i := range shaPwd {
		shaPwd[i] ^= scramble[i]
	}

	return shaPwd
}

func ValidatePasswordWithAuthResponse(rawDb *mysql_db.MySQLDb, user, password string) error {
	salt, err := rawDb.Salt()
	if err != nil {
		return err
	}

	authResponse := buildAuthResponse(salt, password)
	return passwordValidate(rawDb, salt, user, authResponse)
}

// GetDoltStatus retrieves the status of the current working set of changes in the working set, and returns two
// lists of modified tables: staged and unstaged. If both lists are empty, there are no changes in the working set.
// The list of unstaged tables does not include tables that are ignored, as configured by the dolt_ignore table.
func GetDoltStatus(queryist cli.Queryist, sqlCtx *sql.Context) (stagedChangedTables map[string]bool, unstagedChangedTables map[string]bool, err error) {
	stagedChangedTables = make(map[string]bool)
	unstagedChangedTables = make(map[string]bool)
	err = nil

	ignoredPatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return stagedChangedTables, unstagedChangedTables, fmt.Errorf("error: failed to get ignored table patterns: %w", err)
	}

	var statusRows []sql.Row
	statusRows, err = GetRowsForSql(queryist, sqlCtx, "select * from dolt_status;")
	if err != nil {
		return stagedChangedTables, unstagedChangedTables, fmt.Errorf("error: failed to get dolt status: %w", err)
	}

	for _, row := range statusRows {
		tableName := row[0].(string)
		staged := row[1]
		var isStaged bool
		isStaged, err = GetTinyIntColAsBool(staged)
		if err != nil {
			return
		}
		if isStaged {
			stagedChangedTables[tableName] = true
		} else {
			// filter out ignored tables from untracked tables
			ignored, err := ignoredPatterns.IsTableNameIgnored(tableName)
			if conflict := doltdb.AsDoltIgnoreInConflict(err); conflict != nil {
				continue
			} else if err != nil {
				return stagedChangedTables, unstagedChangedTables, fmt.Errorf("error: failed to check if table '%s' is ignored: %w", tableName, err)
			} else if ignored == doltdb.DontIgnore {
				// no-op
			} else if ignored == doltdb.Ignore {
				continue
			} else {
				return stagedChangedTables, unstagedChangedTables, fmt.Errorf("unrecognized ignore result value: %v", ignored)
			}

			unstagedChangedTables[tableName] = true
		}
	}

	return stagedChangedTables, unstagedChangedTables, nil
}
