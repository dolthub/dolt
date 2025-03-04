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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

type CommitInfo struct {
	commitMeta        *datas.CommitMeta
	commitHash        string
	isHead            bool
	parentHashes      []string
	height            uint64
	localBranchNames  []string
	remoteBranchNames []string
	tagNames          []string
}

var fwtStageName = "fwt"

func GetWorkingWithVErr(dEnv *env.DoltEnv) (doltdb.RootValue, errhand.VerboseError) {
	working, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get working.").AddCause(err).Build()
	}

	return working, nil
}

func GetStagedWithVErr(dEnv *env.DoltEnv) (doltdb.RootValue, errhand.VerboseError) {
	staged, err := dEnv.StagedRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	return staged, nil
}

func UpdateWorkingWithVErr(dEnv *env.DoltEnv, updatedRoot doltdb.RootValue) errhand.VerboseError {
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
func NewArgFreeCliContext(ctx context.Context, dEnv *env.DoltEnv, cwd filesys.Filesys) (cli.CliContext, errhand.VerboseError) {
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
	return cli.NewCliContext(argparser.NewEmptyResults(), dEnv.Config, cwd, lateBind)
}

// BuildSqlEngineQueryist Utility function to build a local SQLEngine for use interacting with data on disk using
// SQL queries. ctx, cwdFS, mrEnv, and apr must all be non-nil.
func BuildSqlEngineQueryist(ctx context.Context, cwdFS filesys.Filesys, mrEnv *env.MultiRepoEnv, creds *cli.UserPassword, apr *argparser.ArgParseResults) (cli.LateBindQueryist, errhand.VerboseError) {
	if ctx == nil || cwdFS == nil || mrEnv == nil || creds == nil || apr == nil {
		return nil, errhand.VerboseErrorFromError(fmt.Errorf("Invariant violated. Nil argument provided to BuildSqlEngineQueryist"))
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
	useBranch, hasBranch := apr.GetValue(cli.BranchParam)
	if !hasDB {
		database = mrEnv.GetFirstDatabase()
	}
	if hasBranch {
		dbName, _ := dsess.SplitRevisionDbName(database)
		database = dbName + "/" + useBranch
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
		// We've deferred loading the database as long as we can.
		// If we're binding the Queryist, that means that engine is actually
		// going to be used.
		mrEnv.ReloadDBs(ctx2)

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
		salt, err := mysql.NewSalt()
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
			// Ensure a root user exists, with superuser privs
			dbUser = DefaultUser
			ed := rawDb.Editor()
			defer ed.Close()
			rawDb.AddEphemeralSuperUser(ed, dbUser, config.ServerHost, "")
		}

		// Set client to specified user
		sqlCtx.Session.SetClient(sql.Client{User: dbUser, Address: config.ServerHost, Capabilities: 0})
		return se, sqlCtx, func() { se.Close() }, nil
	}

	return lateBinder, nil
}

func GetRowsForSql(queryist cli.Queryist, sqlCtx *sql.Context, query string) ([]sql.Row, error) {
	_, rowIter, _, err := queryist.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, rowIter)
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

// getUint64ColAsUint64 returns the value of an uint64 column as a string
// This is necessary because Queryist may return an uint64 column as an uint64 (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func getUint64ColAsUint64(col interface{}) (uint64, error) {
	switch v := col.(type) {
	case int:
		return uint64(v), nil
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case string:
		uiv, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return uiv, nil
	default:
		return 0, fmt.Errorf("unexpected type %T, was expecting int64, uint64 or string", v)
	}
}

// getStrBoolColAsBool returns the value of the input as a bool. This is required because depending on if we
// go over the wire or not we may get a string or a bool when we expect a bool.
func getStrBoolColAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return col.(bool), nil
	case string:
		return strings.EqualFold(col.(string), "true") || strings.EqualFold(col.(string), "1"), nil
	default:
		return false, fmt.Errorf("unexpected type %T, was expecting bool or string", v)
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
	salt, err := mysql.NewSalt()
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
	statusRows, err = GetRowsForSql(queryist, sqlCtx, "select table_name,staged from dolt_status;")
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
			ignored, err := ignoredPatterns.IsTableNameIgnored(doltdb.TableName{Name: tableName})
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

// PrintCommitInfo prints the given commit in the format used by log and show.
func PrintCommitInfo(pager *outputpager.Pager, minParents int, showParents, showSignatures bool, decoration string, comm *CommitInfo) {
	color.NoColor = false
	if len(comm.parentHashes) < minParents {
		return
	}

	chStr := comm.commitHash
	if showParents {
		chStr = strings.Join(append([]string{chStr}, comm.parentHashes...), " ")
	}

	// Write commit hash
	pager.Writer.Write([]byte(color.YellowString("commit %s ", chStr))) // Use Dim Yellow (33m)

	// Show decoration
	if decoration != "no" {
		printRefs(pager, comm, decoration)
	}

	if len(comm.parentHashes) > 1 {
		pager.Writer.Write([]byte(fmt.Sprintf("\nMerge:")))
		for _, h := range comm.parentHashes {
			pager.Writer.Write([]byte(" " + h))
		}
	}

	if showSignatures && len(comm.commitMeta.Signature) > 0 {
		signatureLines := strings.Split(comm.commitMeta.Signature, "\n")
		for _, line := range signatureLines {
			pager.Writer.Write([]byte("\n"))
			pager.Writer.Write([]byte(color.CyanString(line)))
		}
	}

	pager.Writer.Write([]byte(fmt.Sprintf("\nAuthor: %s <%s>", comm.commitMeta.Name, comm.commitMeta.Email)))

	timeStr := comm.commitMeta.FormatTS()
	pager.Writer.Write([]byte(fmt.Sprintf("\nDate:  %s", timeStr)))

	formattedDesc := "\n\n\t" + strings.Replace(comm.commitMeta.Description, "\n", "\n\t", -1) + "\n\n"
	pager.Writer.Write([]byte(fmt.Sprintf("%s", formattedDesc)))

}

// printRefs prints the refs associated with the commit in the formatting used by log and show.
func printRefs(pager *outputpager.Pager, comm *CommitInfo, decoration string) {
	// Do nothing if no associate branchNames
	if len(comm.localBranchNames) == 0 && len(comm.remoteBranchNames) == 0 && len(comm.tagNames) == 0 {
		return
	}

	references := []string{}

	for _, b := range comm.localBranchNames {
		if decoration == "full" {
			b = "refs/heads/" + b
		}
		// branch names are bright green (32;1m)
		branchName := color.HiGreenString(b)
		references = append(references, branchName)
	}
	for _, b := range comm.remoteBranchNames {
		if decoration == "full" {
			b = "refs/remotes/" + b
		}
		// remote names are bright red (31;1m)
		branchName := color.HiRedString(b)
		references = append(references, branchName)
	}
	for _, t := range comm.tagNames {
		if decoration == "full" {
			t = "refs/tags/" + t
		}
		// tag names are bright yellow (33;1m)

		tagName := color.HiYellowString("tag: %s", t)
		references = append(references, tagName)
	}

	yellow := color.New(color.FgYellow)
	boldCyan := color.New(color.FgCyan, color.Bold)

	pager.Writer.Write([]byte(yellow.Sprintf("(")))

	if comm.isHead {
		pager.Writer.Write([]byte(boldCyan.Sprintf("HEAD -> ")))
	}

	joinedReferences := strings.Join(references, yellow.Sprint(", "))
	pager.Writer.Write([]byte(yellow.Sprintf("%s) ", joinedReferences)))
}

type commitInfoOptions struct {
	showSignature bool
}

// getCommitInfo returns the commit info for the given ref.
func getCommitInfo(queryist cli.Queryist, sqlCtx *sql.Context, ref string) (*CommitInfo, error) {
	return getCommitInfoWithOptions(queryist, sqlCtx, ref, commitInfoOptions{})
}

func getCommitInfoWithOptions(queryist cli.Queryist, sqlCtx *sql.Context, ref string, opts commitInfoOptions) (*CommitInfo, error) {
	hashOfHead, err := getHashOf(queryist, sqlCtx, "HEAD")
	if err != nil {
		return nil, fmt.Errorf("error getting hash of HEAD: %v", err)
	}

	var q string
	if opts.showSignature {
		q, err = dbr.InterpolateForDialect("select * from dolt_log(?, '--parents', '--decorate=full', '--show-signature')", []interface{}{ref}, dialect.MySQL)
		if err != nil {
			return nil, fmt.Errorf("error interpolating query: %v", err)
		}
	} else {
		q, err = dbr.InterpolateForDialect("select * from dolt_log(?, '--parents', '--decorate=full')", []interface{}{ref}, dialect.MySQL)
		if err != nil {
			return nil, fmt.Errorf("error interpolating query: %v", err)
		}
	}

	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, fmt.Errorf("error getting logs for ref '%s': %v", ref, err)
	}
	if len(rows) == 0 {
		// No commit with this hash exists
		return nil, nil
	}

	row := rows[0]
	commitHash := row[0].(string)
	name := row[1].(string)
	email := row[2].(string)
	timestamp, err := getTimestampColAsUint64(row[3])
	if err != nil {
		return nil, fmt.Errorf("error parsing timestamp '%s': %v", row[3], err)
	}
	message := row[4].(string)
	parent := row[5].(string)
	height := uint64(len(rows))

	isHead := commitHash == hashOfHead

	var signature string
	if len(row) > 7 {
		signature = row[7].(string)
	}

	localBranchesForHash, err := getBranchesForHash(queryist, sqlCtx, commitHash, true)
	if err != nil {
		return nil, fmt.Errorf("error getting branches for hash '%s': %v", commitHash, err)
	}
	remoteBranchesForHash, err := getBranchesForHash(queryist, sqlCtx, commitHash, false)
	if err != nil {
		return nil, fmt.Errorf("error getting remote branches for hash '%s': %v", commitHash, err)
	}
	tagsForHash, err := getTagsForHash(queryist, sqlCtx, commitHash)
	if err != nil {
		return nil, fmt.Errorf("error getting tags for hash '%s': %v", commitHash, err)
	}

	ci := &CommitInfo{
		commitMeta: &datas.CommitMeta{
			Name:          name,
			Email:         email,
			Timestamp:     timestamp,
			Description:   message,
			UserTimestamp: int64(timestamp),
			Signature:     signature,
		},
		commitHash:        commitHash,
		height:            height,
		isHead:            isHead,
		localBranchNames:  localBranchesForHash,
		remoteBranchNames: remoteBranchesForHash,
		tagNames:          tagsForHash,
	}

	if parent != "" {
		ci.parentHashes = strings.Split(parent, ", ")
	}

	return ci, nil
}

func getBranchesForHash(queryist cli.Queryist, sqlCtx *sql.Context, targetHash string, getLocalBranches bool) ([]string, error) {
	var q string
	if getLocalBranches {
		q = "select name, hash from dolt_branches where hash = ?"
	} else {
		q = "select name, hash from dolt_remote_branches where hash = ?"
	}
	q, err := dbr.InterpolateForDialect(q, []interface{}{targetHash}, dialect.MySQL)
	if err != nil {
		return nil, err
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	branches := []string{}
	for _, row := range rows {
		name := row[0].(string)
		branches = append(branches, name)
	}
	return branches, nil
}

func getTagsForHash(queryist cli.Queryist, sqlCtx *sql.Context, targetHash string) ([]string, error) {
	q, err := dbr.InterpolateForDialect("select tag_name from dolt_tags where tag_hash = ?", []interface{}{targetHash}, dialect.MySQL)
	if err != nil {
		return nil, err
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	tags := []string{}
	for _, row := range rows {
		name := row[0].(string)
		tags = append(tags, name)
	}
	return tags, nil
}

// getFastforward helper functions which takes a single sql.Row and an index. If that value at that index is 1, TRUE
// is returned. This is somewhat context specific, but is used to determine if a merge resulted in a fastward, and the
// procedures which do this return the FF flag in different columns of their results.
func getFastforward(row sql.Row, index int) bool {
	fastForward := false
	if row != nil && len(row) > index {
		if ff, ok := row[index].(int64); ok {
			fastForward = ff == 1
		} else if ff, ok := row[index].(string); ok {
			// remote execution returns row as a string
			fastForward = ff == "1"
		}
	}
	return fastForward
}

func getHashOf(queryist cli.Queryist, sqlCtx *sql.Context, ref string) (string, error) {
	q, err := dbr.InterpolateForDialect("select dolt_hashof(?)", []interface{}{ref}, dialect.MySQL)
	if err != nil {
		return "", fmt.Errorf("error interpolating hashof query: %v", err)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return "", fmt.Errorf("error getting hash of ref '%s': %v", ref, err)
	}
	if len(rows) == 0 {
		return "", fmt.Errorf("no commits found for ref %s", ref)
	}
	return rows[0][0].(string), nil
}

// ParseArgsOrPrintHelp is used by most commands to parse arguments and print help if necessary. It's a wrapper around
// cli.ParseArgs that returns an argparser.ArgParseResults IFF parsing the given arguments was successful with the
// provided argparser.ArgParser. Additional return values are a usage printer, a boolean indicating whether the command
// should terminate, and an exit status code.
//
// The caller of this method should check the boolean value to determine if the command should terminate. Termination
// does not necessarily mean an error occurred. For example, if the user requested help, the command should terminate
// with a successful exit code and print the help message with the provided usage printer.
//
// There may be cases where the usage printer is still used by the caller. For example if the arguments parse but they
// don't make sense in the context of other arguments, the caller may want to print the usage message and exit with an
// error code.
func ParseArgsOrPrintHelp(
	ap *argparser.ArgParser,
	commandStr string,
	args []string,
	docs cli.CommandDocumentationContent) (apr *argparser.ArgParseResults, usage cli.UsagePrinter, terminate bool, exitStatus int) {
	helpPrt, usagePrt := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, docs, ap))
	var err error
	apr, err = cli.ParseArgs(ap, args, helpPrt)
	if err != nil {
		if err == argparser.ErrHelp {
			return nil, usagePrt, true, 0
		}
		return nil, usagePrt, true, HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usagePrt)
	}
	return apr, usagePrt, false, 0
}

func HandleVErrAndExitCode(verr errhand.VerboseError, usage cli.UsagePrinter) int {
	if verr != nil {
		if msg := verr.Verbose(); strings.TrimSpace(msg) != "" {
			cli.PrintErrln(msg)
		}

		if verr.ShouldPrintUsage() {
			usage()
		}

		return 1
	}

	return 0
}

func HandleDEnvErrorsAndExitCode(errorBuilder *errhand.DErrorBuilder, dEnv *env.DoltEnv, usage cli.UsagePrinter) bool {
	errorCount := 0
	handleError := func(err error) {
		if err != nil {
			var verboseError errhand.VerboseError
			if errorBuilder != nil {
				verboseError = errorBuilder.AddCause(err).Build()
			} else {
				verboseError = errhand.VerboseErrorFromError(err)
			}
			HandleVErrAndExitCode(verboseError, usage)
			errorCount++
		}
	}
	handleError(dEnv.CfgLoadErr)
	handleError(dEnv.RSLoadErr)
	handleError(dEnv.DBLoadError)

	return errorCount > 0
}

// interpolateStoredProcedureCall returns an interpolated query to call |storedProcedureName| with the arguments
// |args|.
func interpolateStoredProcedureCall(storedProcedureName string, args []string) (string, error) {
	query := fmt.Sprintf("CALL %s(%s);", storedProcedureName, buildPlaceholdersString(len(args)))
	return dbr.InterpolateForDialect(query, stringSliceToInterfaceSlice(args), dialect.MySQL)
}

// stringSliceToInterfaceSlice converts the string slice |ss| into an interface slice with the same values.
func stringSliceToInterfaceSlice(ss []string) []interface{} {
	retSlice := make([]interface{}, 0, len(ss))
	for _, s := range ss {
		retSlice = append(retSlice, s)
	}
	return retSlice
}

// buildPlaceholdersString returns a placeholder string to use in an interpolated query with the specified
// |count| of parameter placeholders.
func buildPlaceholdersString(count int) string {
	return strings.Join(make([]string, count), "?, ") + "?"
}

func PrintStagingError(err error) {
	vErr := func() errhand.VerboseError {
		switch {
		case doltdb.IsRootValUnreachable(err):
			rt := doltdb.GetUnreachableRootType(err)
			bdr := errhand.BuildDError("Unable to read %s.", rt.String())
			bdr.AddCause(doltdb.GetUnreachableRootCause(err))
			return bdr.Build()

		case actions.IsTblNotExist(err):
			tbls := actions.GetTablesForError(err)
			bdr := errhand.BuildDError("Some of the specified tables or docs were not found")
			bdr.AddDetails("Unknown tables or docs: %v", tbls)

			return bdr.Build()

		case actions.IsTblInConflict(err) || actions.IsTblViolatesConstraints(err):
			tbls := actions.GetTablesForError(err)
			bdr := errhand.BuildDError("error: not all tables merged")

			for _, tbl := range tbls {
				bdr.AddDetails("  %s", tbl)
			}

			return bdr.Build()
		case doltdb.AsDoltIgnoreInConflict(err) != nil:
			return errhand.VerboseErrorFromError(err)
		default:
			return errhand.BuildDError("Unknown error").AddCause(err).Build()
		}
	}()

	cli.PrintErrln(vErr.Verbose())
}

// execEditor opens editor to ask user for input.
func execEditor(initialMsg string, suffix string, cliCtx cli.CliContext) (editedMsg string, err error) {
	if cli.ExecuteWithStdioRestored == nil {
		return initialMsg, nil
	}

	if !checkIsTerminal() {
		return initialMsg, nil
	}

	backupEd := "vim"
	// try getting default editor on the user system
	if ed, edSet := os.LookupEnv(dconfig.EnvEditor); edSet {
		backupEd = ed
	}
	// try getting Dolt config core.editor
	editorStr := cliCtx.Config().GetStringOrDefault(config.DoltEditor, backupEd)

	cli.ExecuteWithStdioRestored(func() {
		editedMsg, err = editor.OpenTempEditor(editorStr, initialMsg, suffix)
		if err != nil {
			return
		}
	})

	if err != nil {
		return "", fmt.Errorf("Failed to open commit editor: %v \n Check your `EDITOR` environment variable with `echo $EDITOR` or your dolt config with `dolt config --list` to ensure that your editor is valid", err)
	}

	return editedMsg, nil
}
