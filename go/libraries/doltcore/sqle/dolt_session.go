// Copyright 2020 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	HeadKeySuffix    = "_head"
	HeadRefKeySuffix = "_head_ref"
	WorkingKeySuffix = "_working"
	StagedKeySuffix  = "_staged"
)

const (
	EnableTransactionsEnvKey      = "DOLT_ENABLE_TRANSACTIONS"
	DoltCommitOnTransactionCommit = "dolt_transaction_commit"
)

type batchMode int8

const (
	single batchMode = iota
	batched
)

const TransactionsEnabledSysVar = "dolt_transactions_enabled"

func init() {
	txEnabledSessionVar := int8(1)
	enableTx, ok := os.LookupEnv(EnableTransactionsEnvKey)
	if ok {
		if strings.ToLower(enableTx) == "true" {
			txEnabledSessionVar = int8(1)
		}
	}
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{
			Name:              DoltCommitOnTransactionCommit,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(DoltCommitOnTransactionCommit),
			Default:           int8(0),
		},
		{
			Name:              TransactionsEnabledSysVar,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(TransactionsEnabledSysVar),
			Default:           txEnabledSessionVar,
		},
	})
}

func TransactionsEnabled(ctx *sql.Context) bool {
	enabled, err := ctx.GetSessionVariable(ctx, TransactionsEnabledSysVar)
	if err != nil {
		panic(err)
	}

	switch enabled.(int8) {
	case 0:
		return false
	case 1:
		return true
	default:
		panic(fmt.Sprintf("Unexpected value %v", enabled))
	}
}

func IsHeadKey(key string) (bool, string) {
	if strings.HasSuffix(key, HeadKeySuffix) {
		return true, key[:len(key)-len(HeadKeySuffix)]
	}

	return false, ""
}

func IsWorkingKey(key string) (bool, string) {
	if strings.HasSuffix(key, WorkingKeySuffix) {
		return true, key[:len(key)-len(WorkingKeySuffix)]
	}

	return false, ""
}

// DoltSession is the sql.Session implementation used by dolt.  It is accessible through a *sql.Context instance
type DoltSession struct {
	sql.Session
	batchMode batchMode
	Username  string
	Email     string
	dbStates  map[string]*DatabaseSessionState
}

type DatabaseSessionState struct {
	dbName               string
	headCommit           *doltdb.Commit
	detachedHead         bool
	headRoot             *doltdb.RootValue
	workingSet           *doltdb.WorkingSet
	dbData               env.DbData
	editSession          *editor.TableEditSession
	dirty                bool
	tempTableRoot        *doltdb.RootValue
	tempTableEditSession *editor.TableEditSession
}

func (d DatabaseSessionState) GetRoots() doltdb.Roots {
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.workingSet.WorkingRoot(),
		Staged:  d.workingSet.StagedRoot(),
	}
}

var _ sql.Session = &DoltSession{}

// DefaultDoltSession creates a DoltSession object with default values
func DefaultDoltSession() *DoltSession {
	sess := &DoltSession{
		Session:  sql.NewBaseSession(),
		Username: "",
		Email:    "",
		dbStates: make(map[string]*DatabaseSessionState),
	}
	return sess
}

type InitialDbState struct {
	Db         sql.Database
	HeadCommit *doltdb.Commit
	WorkingSet *doltdb.WorkingSet
	DbData     env.DbData
}

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(ctx *sql.Context, sqlSess sql.Session, username, email string, dbs ...InitialDbState) (*DoltSession, error) {

	// TODO: kill this with fire
	dbDatas := make(map[string]env.DbData)
	editSessions := make(map[string]*editor.TableEditSession)

	for _, state := range dbs {
		db, ok := state.Db.(Database)
		if !ok {
			return nil, fmt.Errorf("expected a sqle.Database, but got %T", state.Db)
		}

		dbDatas[db.Name()] = env.DbData{Rsw: db.rsw, Ddb: db.ddb, Rsr: db.rsr, Drw: db.drw}
		editSessions[db.Name()] = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})
	}

	sess := &DoltSession{
		Session:  sqlSess,
		Username: username,
		Email:    email,
		dbStates: make(map[string]*DatabaseSessionState),
	}

	for _, db := range dbs {
		err := sess.AddDB(ctx, db)

		if err != nil {
			return nil, err
		}
	}

	return sess, nil
}

// EnableBatchedMode enables batched mode for this session. This is only safe to do during initialization.
// Sessions operating in batched mode don't flush any edit buffers except when told to do so explicitly, or when a
// transaction commits. Disable @@autocommit to prevent edit buffers from being flushed prematurely in this mode.
func (sess *DoltSession) EnableBatchedMode() {
	sess.batchMode = batched
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}

// Flush flushes all changes sitting in edit sessions to the session root for the database named. This normally
// happens automatically as part of statement execution, and is only necessary when the session is manually batched (as
// for bulk SQL import)
func (sess *DoltSession) Flush(ctx *sql.Context, dbName string) error {
	editSession := sess.dbStates[dbName].editSession
	newRoot, err := editSession.Flush(ctx)
	if err != nil {
		return err
	}

	return sess.SetRoot(ctx, dbName, newRoot)
}

// CommitTransaction commits the in-progress transaction for the database named
func (sess *DoltSession) CommitTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if sess.batchMode == batched {
		err := sess.Flush(ctx, dbName)
		if err != nil {
			return err
		}
	}

	if !sess.dbStates[dbName].dirty {
		return nil
	}

	// This is triggered when certain commands are sent to the server (ex. commit) when a database is not selected.
	// These commands should not error.
	if dbName == "" {
		return nil
	}

	dbstate, ok := sess.dbStates[dbName]
	// It's possible that this returns false if the user has created an in-Memory database. Moreover,
	// the analyzer will check for us whether a db exists or not.
	// TODO: fix this
	if !ok {
		return nil
	}

	// Old "commit" path, which just writes whatever the root for this session is to the repo state file with no care
	// for concurrency. Over time we will disable this path.
	// TODO: remove me
	if !TransactionsEnabled(ctx) {
		dbData := dbstate.dbData
		dbstate.dirty = false
		return dbData.Rsw.UpdateWorkingRoot(ctx, dbstate.GetRoots().Working)
	}

	// Newer commit path does a concurrent merge of the current root with the one other clients are editing, then
	// updates the session with this new root.
	// TODO: validate that the transaction belongs to the DB named
	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	// TODO: actual logging
	// logrus.Errorf("working root to commit is %s", dbstate.workingSet.WorkingRoot().DebugString(ctx, true))

	mergedWorkingSet, err := dtx.Commit(ctx, dbstate.workingSet)
	if err != nil {
		return err
	}

	// logrus.Errorf("committed merged working root %s", dbstate.workingSet.WorkingRoot().DebugString(ctx, true))

	err = sess.SetWorkingSet(ctx, dbName, mergedWorkingSet, nil)
	if err != nil {
		return err
	}

	err = sess.CommitWorkingSetToDolt(ctx, dtx.dbData, dbName)
	if err != nil {
		return err
	}

	dbstate.dirty = false
	return nil
}

// CommitWorkingSetToDolt stages the working set and then immediately commits the staged changes. This is a Dolt commit
// rather than a transaction commit. If there are no changes to be staged, then no commit is created.
func (sess *DoltSession) CommitWorkingSetToDolt(ctx *sql.Context, dbData env.DbData, dbName string) error {
	return nil

	// TODO: fix me
	// if commitBool, err := sess.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommit); err != nil {
	// 	return err
	// } else if commitBool.(int8) == 1 {
	// 	fkChecks, err := sess.Session.GetSessionVariable(ctx, "foreign_key_checks")
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	workingRoot := sess.roots[dbName].root
	//
	// 	err = actions.StageAllTables(ctx, workingRoot, dbData)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	queryTime := ctx.QueryTime()
	// 	_, err = actions.CommitStaged(ctx, workingRoot, dbData, actions.CommitStagedProps{
	// 		Message:          fmt.Sprintf("Transaction commit at %s", queryTime.UTC().Format("2006-01-02T15:04:05Z")),
	// 		Date:             queryTime,
	// 		AllowEmpty:       false,
	// 		CheckForeignKeys: fkChecks.(int8) == 1,
	// 		Name:             sess.Username,
	// 		Email:            sess.Email,
	// 	})
	// 	if _, ok := err.(actions.NothingStaged); err != nil && !ok {
	// 		return err
	// 	}
	//
	// 	headCommit, err := dbData.Ddb.Resolve(ctx, dbData.Rsr.CWBHeadSpec(), dbData.Rsr.CWBHeadRef())
	// 	if err != nil {
	// 		return err
	// 	}
	// 	headHash, err := headCommit.HashOf()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = sess.Session.SetSessionVariable(ctx, HeadKey(dbName), headHash.String())
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// return nil
}

// RollbackTransaction rolls the given transaction back
func (sess *DoltSession) RollbackTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled(ctx) || dbName == "" {
		return nil
	}

	if !sess.dbStates[dbName].dirty {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	err := sess.SetRoot(ctx, dbName, dtx.startState.WorkingRoot())
	if err != nil {
		return err
	}

	sess.dbStates[dbName].dirty = false
	return nil
}

// CreateSavepoint creates a new savepoint for this transaction with the name given. A previously created savepoint
// with the same name will be overwritten.
func (sess *DoltSession) CreateSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled(ctx) || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	dtx.CreateSavepoint(savepointName, sess.dbStates[dbName].GetRoots().Working)
	return nil
}

// RollbackToSavepoint sets this session's root to the one saved in the savepoint name. It's an error if no savepoint
// with that name exists.
func (sess *DoltSession) RollbackToSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled(ctx) || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	root := dtx.RollbackToSavepoint(savepointName)
	if root == nil {
		return sql.ErrSavepointDoesNotExist.New(savepointName)
	}

	err := sess.SetRoot(ctx, dbName, root)
	if err != nil {
		return err
	}

	return nil
}

// ReleaseSavepoint removes the savepoint name from the transaction. It's an error if no savepoint with that name
// exists.
func (sess *DoltSession) ReleaseSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled(ctx) || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	root := dtx.ClearSavepoint(savepointName)
	if root == nil {
		return sql.ErrSavepointDoesNotExist.New(savepointName)
	}

	return nil
}

// GetDoltDB returns the *DoltDB for a given database by name
func (sess *DoltSession) GetDoltDB(dbName string) (*doltdb.DoltDB, bool) {
	dbstate, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	return dbstate.dbData.Ddb, true
}

func (sess *DoltSession) GetDoltDBRepoStateWriter(dbName string) (env.RepoStateWriter, bool) {
	d, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Rsw, true
}

func (sess *DoltSession) GetDoltDBRepoStateReader(dbName string) (env.RepoStateReader, bool) {
	d, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Rsr, true
}

func (sess *DoltSession) GetDoltDBDocsReadWriter(dbName string) (env.DocsReadWriter, bool) {
	d, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Drw, true
}

func (sess *DoltSession) GetDbData(dbName string) (env.DbData, bool) {
	ddb, ok := sess.GetDoltDB(dbName)

	if !ok {
		return env.DbData{}, false
	}

	rsr, ok := sess.GetDoltDBRepoStateReader(dbName)

	if !ok {
		return env.DbData{}, false
	}

	rsw, ok := sess.GetDoltDBRepoStateWriter(dbName)

	if !ok {
		return env.DbData{}, false
	}

	drw, ok := sess.GetDoltDBDocsReadWriter(dbName)

	if !ok {
		return env.DbData{}, false
	}

	return env.DbData{
		Ddb: ddb,
		Rsr: rsr,
		Rsw: rsw,
		Drw: drw,
	}, true
}

// GetRoot returns the current working *RootValue for a given database associated with the session
func (sess *DoltSession) GetRoot(dbName string) (*doltdb.RootValue, bool) {
	dbstate, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	return dbstate.GetRoots().Working, true
}

// GetRoots returns the current roots for a given database associated with the session
func (sess *DoltSession) GetRoots(dbName string) (doltdb.Roots, bool) {
	dbstate, ok := sess.dbStates[dbName]
	if !ok {
		return doltdb.Roots{}, false
	}

	return dbstate.GetRoots(), true
}

// SetRoot sets a new root value for the session for the database named. This is the primary mechanism by which data
// changes are communicated to the engine and persisted back to disk. All data changes should be followed by a call to
// update the session's root value via this method.
// Data changes contained in the |newRoot| aren't persisted until this session is committed.
// TODO: rename to SetWorkingRoot
func (sess *DoltSession) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sessionState := sess.dbStates[dbName]
	if rootsEqual(sessionState.GetRoots().Working, newRoot) {
		return nil
	}

	return sess.setRoot(ctx, dbName, newRoot)
}

// setRoot is like its exported version, but skips the consistency check
func (sess *DoltSession) setRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sessionState := sess.dbStates[dbName]

	h, err := newRoot.HashOf()
	if err != nil {
		return err
	}

	hashStr := h.String()
	err = sess.Session.SetSessionVariable(ctx, WorkingKey(dbName), hashStr)
	if err != nil {
		return err
	}

	sessionState.workingSet = sessionState.workingSet.WithWorkingRoot(newRoot)

	err = sessionState.editSession.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	sessionState.dirty = true
	return nil
}

// SetRoots sets new roots for the session for the database named. Typically clients should only set the working root,
// via setRoot. This method is for clients that need to update more of the session state, such as the dolt_ functions.
// Unlike setting the only the working root, this method always marks the database state dirty.
func (sess *DoltSession) SetRoots(ctx *sql.Context, dbName string, roots doltdb.Roots) error {
	// TODO: handle HEAD here?
	workingSet := sess.dbStates[dbName].workingSet.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged)
	return sess.SetWorkingSet(ctx, dbName, workingSet, nil)
}

// SetWorkingSet sets the working set for this session.  Unlike setting the working root alone, this method always
// marks the session dirty.
// |headRoot| will be set to the working sets's corresponding HEAD if nil
func (sess *DoltSession) SetWorkingSet(
	ctx *sql.Context,
	dbName string,
	ws *doltdb.WorkingSet,
	headRoot *doltdb.RootValue,
) error {
	sessionState := sess.dbStates[dbName]
	sessionState.workingSet = ws

	if headRoot == nil && !sessionState.detachedHead {
		cs, err := doltdb.NewCommitSpec(ws.Ref().GetPath())
		if err != nil {
			return err
		}

		cm, err := sessionState.dbData.Ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}

		sessionState.headCommit = cm

		headRoot, err = cm.GetRootValue()
		if err != nil {
			return err
		}
	}

	if headRoot != nil {
		sessionState.headRoot = headRoot
	}

	err := sess.setSessionVarsForDb(ctx, dbName)
	if err != nil {
		return err
	}

	// setRoot updates any edit sessions in use
	err = sess.setRoot(ctx, dbName, ws.WorkingRoot())
	if err != nil {
		return nil
	}

	return nil
}

func (sess *DoltSession) WorkingSet(ctx *sql.Context, dbName string) *doltdb.WorkingSet {
	sessionState := sess.dbStates[dbName]
	return sessionState.workingSet
}

func (sess *DoltSession) GetTempTableRootValue(ctx *sql.Context, dbName string) (*doltdb.RootValue, bool) {
	dbstate, ok := sess.dbStates[dbName]
	if !ok {
		return nil, false
	}

	if dbstate.tempTableRoot == nil {
		return nil, false
	}

	return dbstate.tempTableRoot, true
}

func (sess *DoltSession) SetTempTableRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sess.dbStates[dbName].tempTableRoot = newRoot
	return sess.dbStates[dbName].tempTableEditSession.SetRoot(ctx, newRoot)
}

// GetHeadCommit returns the parent commit of the current session.
func (sess *DoltSession) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, error) {
	dbState, dbFound := sess.dbStates[dbName]
	if !dbFound {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return dbState.headCommit, nil
}

// SetSessionVariable is defined on sql.Session. We intercept it here to interpret the special semantics of the system
// vars that we define. Otherwise we pass it on to the base implementation.
func (sess *DoltSession) SetSessionVariable(ctx *sql.Context, key string, value interface{}) error {
	// TODO: working set ref

	if isHead, dbName := IsHeadKey(key); isHead {
		err := sess.setHeadSessionVar(ctx, value, dbName)
		if err != nil {
			return err
		}

		sess.dbStates[dbName].detachedHead = true
		return nil
	}

	if isWorking, dbName := IsWorkingKey(key); isWorking {
		return sess.setWorkingSessionVar(ctx, value, dbName)
	}

	// TODO: allow setting staged directly via var? seems like no

	if strings.ToLower(key) == "foreign_key_checks" {
		return sess.setForeignKeyChecksSessionVar(ctx, key, value)
	}

	return sess.Session.SetSessionVariable(ctx, key, value)
}

func (sess *DoltSession) setForeignKeyChecksSessionVar(ctx *sql.Context, key string, value interface{}) error {
	convertedVal, err := sql.Int64.Convert(value)
	if err != nil {
		return err
	}
	intVal := int64(0)
	if convertedVal != nil {
		intVal = convertedVal.(int64)
	}
	if intVal == 0 {
		for _, dbState := range sess.dbStates {
			dbState.editSession.Props.ForeignKeyChecksDisabled = true
		}
	} else if intVal == 1 {
		for _, dbState := range sess.dbStates {
			dbState.editSession.Props.ForeignKeyChecksDisabled = false
		}
	} else {
		return fmt.Errorf("variable 'foreign_key_checks' can't be set to the value of '%d'", intVal)
	}

	return sess.Session.SetSessionVariable(ctx, key, value)
}

func (sess *DoltSession) setWorkingSessionVar(ctx *sql.Context, value interface{}, dbName string) error {
	valStr, isStr := value.(string) // valStr represents a root val hash
	if !isStr || !hash.IsValid(valStr) {
		return doltdb.ErrInvalidHash
	}

	dbstate, dbFound := sess.dbStates[dbName]
	if !dbFound {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	root, err := dbstate.dbData.Ddb.ReadRootValue(ctx, hash.Parse(valStr))
	if errors.Is(doltdb.ErrNoRootValAtHash, err) {
		return nil
	} else if err != nil {
		return err
	}

	return sess.SetRoot(ctx, dbName, root)
}

func (sess *DoltSession) setHeadSessionVar(ctx *sql.Context, value interface{}, dbName string) error {
	dbstate, dbFound := sess.dbStates[dbName]
	if !dbFound {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	valStr, isStr := value.(string)

	if !isStr || !hash.IsValid(valStr) {
		return doltdb.ErrInvalidHash
	}

	cs, err := doltdb.NewCommitSpec(valStr)
	if err != nil {
		return err
	}

	cm, err := dbstate.dbData.Ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	dbstate.headCommit = cm

	root, err := cm.GetRootValue()
	if err != nil {
		return err
	}

	dbstate.headRoot = root

	err = sess.Session.SetSessionVariable(ctx, HeadKey(dbName), value)
	if err != nil {
		return err
	}

	// TODO: preserve working set changes?
	return sess.SetRoot(ctx, dbName, root)
}

// SetSessionVarDirectly directly updates sess.Session. This is useful in the context of the sql shell where
// the working and head session variable may be updated at different times.
func (sess *DoltSession) SetSessionVarDirectly(ctx *sql.Context, key string, value interface{}) error {
	return sess.Session.SetSessionVariable(ctx, key, value)
}

// AddDB adds the database given to this session. This establishes a starting root value for this session, as well as
// other state tracking metadata.
func (sess *DoltSession) AddDB(ctx *sql.Context, dbState InitialDbState) error {
	db := dbState.Db
	defineSystemVariables(db.Name())

	sessionState := &DatabaseSessionState{}
	sess.dbStates[db.Name()] = sessionState

	// TODO: get rid of all repo state writer stuff
	sessionState.dbData = dbState.DbData
	sessionState.editSession = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})

	err := sess.Session.SetSessionVariable(ctx, HeadRefKey(db.Name()), dbState.WorkingSet.Ref().GetPath())
	if err != nil {
		return err
	}

	sessionState.workingSet = dbState.WorkingSet
	workingRoot := dbState.WorkingSet.WorkingRoot()
	logrus.Tracef("working root intialized to %s", workingRoot.DebugString(ctx, false))

	err = sess.setRoot(ctx, db.Name(), workingRoot)
	if err != nil {
		return err
	}

	// This has to happen after SetRoot above, since it does a stale check before its work
	// TODO: this needs to be kept up to date as the working set ref changes
	sessionState.headCommit = dbState.HeadCommit

	headCommitHash, err := dbState.HeadCommit.HashOf()
	if err != nil {
		return err
	}

	err = sess.Session.SetSessionVariable(ctx, HeadKey(db.Name()), headCommitHash.String())
	if err != nil {
		return err
	}

	// After setting the initial root we have no state to commit
	sessionState.dirty = false
	return nil
}

// CreateTemporaryTablesRoot creates an empty root value and a table edit session for the purposes of storing
// temporary tables. This should only be used on demand. That is only when a temporary table is created should we
// create the root map and edit session map.
func (sess *DoltSession) CreateTemporaryTablesRoot(ctx *sql.Context, dbName string, ddb *doltdb.DoltDB) error {
	newRoot, err := doltdb.EmptyRootValue(ctx, ddb.ValueReadWriter())
	if err != nil {
		return err
	}

	sess.dbStates[dbName].tempTableEditSession = editor.CreateTableEditSession(newRoot, editor.TableEditSessionProps{})

	return sess.SetTempTableRoot(ctx, dbName, newRoot)
}

// CWBHeadRef returns the branch ref for this session HEAD for the database named
func (sess *DoltSession) CWBHeadRef(dbName string) (ref.DoltRef, error) {
	return sess.dbStates[dbName].workingSet.Ref().ToHeadRef()
}

// setSessionVarsForDb updates the three session vars that track the value of the session root hashes
func (sess *DoltSession) setSessionVarsForDb(ctx *sql.Context, dbName string) error {
	state := sess.dbStates[dbName]
	roots := state.GetRoots()

	h, err := roots.Working.HashOf()
	if err != nil {
		return err
	}
	err = sess.Session.SetSessionVariable(ctx, WorkingKey(dbName), h.String())
	if err != nil {
		return err
	}

	h, err = roots.Staged.HashOf()
	if err != nil {
		return err
	}
	err = sess.Session.SetSessionVariable(ctx, StagedKey(dbName), h.String())
	if err != nil {
		return err
	}

	h, err = state.headCommit.HashOf()
	if err != nil {
		return err
	}
	err = sess.Session.SetSessionVariable(ctx, HeadKey(dbName), h.String())
	if err != nil {
		return err
	}

	return nil
}

// defineSystemVariables defines dolt-session variables in the engine as necessary
func defineSystemVariables(name string) {
	if _, _, ok := sql.SystemVariables.GetGlobal(name + HeadKeySuffix); !ok {
		sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
			{
				Name:              HeadRefKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(HeadRefKey(name)),
				Default:           "",
			},
			{
				Name:              HeadKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(HeadKey(name)),
				Default:           "",
			},
			{
				Name:              WorkingKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(WorkingKey(name)),
				Default:           "",
			},
			{
				Name:              StagedKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(StagedKey(name)),
				Default:           "",
			},
		})
	}
}
