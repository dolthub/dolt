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

package dsess

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sglobal"
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
	Batched
)

const TransactionsDisabledSysVar = "dolt_transactions_disabled"

func HeadKey(dbName string) string {
	return dbName + HeadKeySuffix
}

func HeadRefKey(dbName string) string {
	return dbName + HeadRefKeySuffix
}

func WorkingKey(dbName string) string {
	return dbName + WorkingKeySuffix
}

func StagedKey(dbName string) string {
	return dbName + StagedKeySuffix
}

func init() {
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
			Name:              TransactionsDisabledSysVar,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(TransactionsDisabledSysVar),
			Default:           int8(0),
		},
	})
}

func TransactionsDisabled(ctx *sql.Context) bool {
	enabled, err := ctx.GetSessionVariable(ctx, TransactionsDisabledSysVar)
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

// Session is the sql.Session implementation used by dolt.  It is accessible through a *sql.Context instance
type Session struct {
	sql.Session
	BatchMode batchMode
	Username  string
	Email     string
	// TODO: make this private again
	DbStates map[string]*DatabaseSessionState
}

type DatabaseSessionState struct {
	dbName               string
	headCommit           *doltdb.Commit
	detachedHead         bool
	headRoot             *doltdb.RootValue
	WorkingSet           *doltdb.WorkingSet
	dbData               env.DbData
	EditSession          *editor.TableEditSession
	dirty                bool
	TempTableRoot        *doltdb.RootValue
	TempTableEditSession *editor.TableEditSession
	GlobalState          sglobal.GlobalState
}

func (d DatabaseSessionState) GetRoots() doltdb.Roots {
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.WorkingSet.WorkingRoot(),
		Staged:  d.WorkingSet.StagedRoot(),
	}
}

var _ sql.Session = &Session{}

// DefaultSession creates a Session object with default values
func DefaultSession() *Session {
	sess := &Session{
		Session:  sql.NewBaseSession(),
		Username: "",
		Email:    "",
		DbStates: make(map[string]*DatabaseSessionState),
	}
	return sess
}

type InitialDbState struct {
	Db          sql.Database
	HeadCommit  *doltdb.Commit
	WorkingSet  *doltdb.WorkingSet
	DbData      env.DbData
	GlobalState sglobal.GlobalState
}

// NewSession creates a Session object from a standard sql.Session and 0 or more Database objects.
func NewSession(ctx *sql.Context, sqlSess sql.Session, username, email string, dbs ...InitialDbState) (*Session, error) {
	sess := &Session{
		Session:  sqlSess,
		Username: username,
		Email:    email,
		DbStates: make(map[string]*DatabaseSessionState),
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
func (sess *Session) EnableBatchedMode() {
	sess.BatchMode = Batched
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *Session {
	return sess.(*Session)
}

// Flush flushes all changes sitting in edit sessions to the session root for the database named. This normally
// happens automatically as part of statement execution, and is only necessary when the session is manually batched (as
// for bulk SQL import)
func (sess *Session) Flush(ctx *sql.Context, dbName string) error {
	editSession := sess.DbStates[dbName].EditSession
	newRoot, err := editSession.Flush(ctx)
	if err != nil {
		return err
	}

	return sess.SetRoot(ctx, dbName, newRoot)
}

// DisabledTransaction is a no-op transaction type that lets us feature-gate transaction logic changes
type DisabledTransaction struct{}

func (d DisabledTransaction) String() string {
	return "Disabled transaction"
}

// CommitTransaction commits the in-progress transaction for the database named
func (sess *Session) StartTransaction(ctx *sql.Context, dbName string) (sql.Transaction, error) {
	if TransactionsDisabled(ctx) {
		return DisabledTransaction{}, nil
	}

	sessionState := sess.DbStates[dbName]

	wsRef := sessionState.WorkingSet.Ref()
	ws, err := sessionState.dbData.Ddb.ResolveWorkingSet(ctx, wsRef)
	if err == doltdb.ErrWorkingSetNotFound {
		ws, err = sess.newWorkingSetForHead(ctx, wsRef, dbName)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// logrus.Tracef("starting transaction with working root %s", ws.WorkingRoot().DebugString(ctx, true))

	// TODO: this is going to do 2 resolves to get the head root, not ideal
	err = sess.SetWorkingSet(ctx, dbName, ws, nil)

	// SetWorkingSet always sets the dirty bit, but by definition we are clean at transaction start
	sessionState.dirty = false

	return NewDoltTransaction(ws, wsRef, sessionState.dbData), nil
}

func (sess *Session) newWorkingSetForHead(ctx *sql.Context, wsRef ref.WorkingSetRef, dbName string) (*doltdb.WorkingSet, error) {
	dbData, _ := sess.GetDbData(dbName)

	headSpec, _ := doltdb.NewCommitSpec("HEAD")
	headRef, err := wsRef.ToHeadRef()
	if err != nil {
		return nil, err
	}

	headCommit, err := dbData.Ddb.Resolve(ctx, headSpec, headRef)
	if err != nil {
		return nil, err
	}

	headRoot, err := headCommit.GetRootValue()
	if err != nil {
		return nil, err
	}

	return doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(headRoot).WithStagedRoot(headRoot), nil
}

// CommitTransaction commits the in-progress transaction for the database named
func (sess *Session) CommitTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if sess.BatchMode == Batched {
		err := sess.Flush(ctx, dbName)
		if err != nil {
			return err
		}
	}

	if TransactionsDisabled(ctx) {
		return nil
	}

	if !sess.DbStates[dbName].dirty {
		return nil
	}

	// This is triggered when certain commands are sent to the server (ex. commit) when a database is not selected.
	// These commands should not error.
	if dbName == "" {
		return nil
	}

	dbstate, ok := sess.DbStates[dbName]
	// It's possible that this returns false if the user has created an in-Memory database. Moreover,
	// the analyzer will check for us whether a db exists or not.
	// TODO: fix this
	if !ok {
		return nil
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

	mergedWorkingSet, err := dtx.Commit(ctx, dbstate.WorkingSet)
	if err != nil {
		return err
	}

	// logrus.Errorf("committed merged working root %s", dbstate.workingSet.WorkingRoot().DebugString(ctx, true))

	err = sess.SetWorkingSet(ctx, dbName, mergedWorkingSet, nil)
	if err != nil {
		return err
	}

	err = sess.CreateDoltCommit(ctx, dbName)
	if err != nil {
		return err
	}

	dbstate.dirty = false
	return nil
}

func (sess *Session) CommitToDolt(
	ctx *sql.Context,
	roots doltdb.Roots,
	dbName string,
	props actions.CommitStagedProps,
) (*doltdb.Commit, error) {
	sessionState := sess.DbStates[dbName]
	dbData := sessionState.dbData

	// TODO: this does several session state updates, and it really needs to just do one
	//  It's also not atomic with the above commit. We need a way to set both new HEAD and update the working
	//  set together, atomically. We can't easily do this in noms right now, because the the data set is the unit of
	//  atomic update at the API layer. There's a root value which is the unit of atomic updates at the storage layer,
	//  just no API which allows one to update more than one dataset in the same atomic transaction. We need to write
	//  one.
	//  Meanwhile, this is all kinds of thread-unsafe
	commit, err := actions.CommitStaged(ctx, roots, dbData, props)
	if err != nil {
		return nil, err
	}

	// Now we have to do *another* SQL transaction, because CommitStaged currently modifies the super schema of the root
	// value before committing what it's given. We need that exact same root in our working set to stay consistent. It
	// doesn't happen automatically like outside the SQL context because CommitStaged is writing to a session-based
	// repo state writer, so we're never persisting the new working set to disk like in a command line context.
	// TODO: fix this mess

	ws := sess.WorkingSet(ctx, dbName)
	// StartTransaction sets the working set for the session, and we want the one we previous had, not the one on disk
	// Updating the working set like this also updates the head commit and root info for the session
	tx, err := sess.StartTransaction(ctx, dbName)
	if err != nil {
		return nil, err
	}

	err = sess.SetWorkingSet(ctx, dbName, ws.ClearMerge(), nil)
	if err != nil {
		return nil, err
	}

	err = sess.CommitTransaction(ctx, dbName, tx)
	if err != nil {
		return nil, err
	}

	// Unsetting the transaction here ensures that it won't be re-committed when this statement concludes
	ctx.SetTransaction(nil)
	return commit, err
}

// CreateDoltCommit stages the working set and then immediately commits the staged changes. This is a Dolt commit
// rather than a transaction commit. If there are no changes to be staged, then no commit is created.
func (sess *Session) CreateDoltCommit(ctx *sql.Context, dbName string) error {
	commitBool, err := sess.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommit)
	if err != nil {
		return err
	} else if commitBool.(int8) != 1 {
		return nil
	}

	fkChecks, err := sess.Session.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}

	sessionState := sess.DbStates[dbName]
	roots := sessionState.GetRoots()

	roots, err = actions.StageAllTablesNoDocs(ctx, roots)
	if err != nil {
		return err
	}

	_, err = sess.CommitToDolt(ctx, roots, dbName, actions.CommitStagedProps{
		Message:          fmt.Sprintf("Transaction commit at %s", ctx.QueryTime().UTC().Format("2006-01-02T15:04:05Z")),
		Date:             ctx.QueryTime(),
		AllowEmpty:       false,
		CheckForeignKeys: fkChecks.(int8) == 1,
		Name:             sess.Username,
		Email:            sess.Email,
	})
	if _, ok := err.(actions.NothingStaged); err != nil && !ok {
		return err
	}

	return nil
}

// RollbackTransaction rolls the given transaction back
func (sess *Session) RollbackTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if !TransactionsDisabled(ctx) || dbName == "" {
		return nil
	}

	if !sess.DbStates[dbName].dirty {
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

	sess.DbStates[dbName].dirty = false
	return nil
}

// CreateSavepoint creates a new savepoint for this transaction with the name given. A previously created savepoint
// with the same name will be overwritten.
func (sess *Session) CreateSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if TransactionsDisabled(ctx) || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	dtx.CreateSavepoint(savepointName, sess.DbStates[dbName].GetRoots().Working)
	return nil
}

// RollbackToSavepoint sets this session's root to the one saved in the savepoint name. It's an error if no savepoint
// with that name exists.
func (sess *Session) RollbackToSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if TransactionsDisabled(ctx) || dbName == "" {
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
func (sess *Session) ReleaseSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if TransactionsDisabled(ctx) || dbName == "" {
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
func (sess *Session) GetDoltDB(dbName string) (*doltdb.DoltDB, bool) {
	dbstate, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	return dbstate.dbData.Ddb, true
}

func (sess *Session) GetDoltDBRepoStateWriter(dbName string) (env.RepoStateWriter, bool) {
	d, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Rsw, true
}

func (sess *Session) GetDoltDBRepoStateReader(dbName string) (env.RepoStateReader, bool) {
	d, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Rsr, true
}

func (sess *Session) GetDoltDBDocsReadWriter(dbName string) (env.DocsReadWriter, bool) {
	d, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	return d.dbData.Drw, true
}

func (sess *Session) GetDoltDbAutoIncrementTracker(dbName string) (sglobal.AutoIncrementTracker, bool) {
	d, ok := sess.DbStates[dbName]

	if !ok {
		return nil, false
	}

	wsref := d.WorkingSet.Ref()

	if d.GlobalState == nil {
		return nil, false
	}

	tracker := d.GlobalState.GetAutoIncrementTracker(wsref)

	return tracker, true
}

func (sess *Session) GetDbData(dbName string) (env.DbData, bool) {
	sessionState, ok := sess.DbStates[dbName]
	if !ok {
		return env.DbData{}, false
	}

	return sessionState.dbData, true
}

// GetRoot returns the current working *RootValue for a given database associated with the session
func (sess *Session) GetRoot(dbName string) (*doltdb.RootValue, bool) {
	dbstate, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	return dbstate.GetRoots().Working, true
}

// GetRoots returns the current roots for a given database associated with the session
func (sess *Session) GetRoots(dbName string) (doltdb.Roots, bool) {
	dbstate, ok := sess.DbStates[dbName]
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
func (sess *Session) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sessionState := sess.DbStates[dbName]
	if rootsEqual(sessionState.GetRoots().Working, newRoot) {
		return nil
	}

	return sess.setRoot(ctx, dbName, newRoot)
}

// setRoot is like its exported version, but skips the consistency check
func (sess *Session) setRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	// logrus.Tracef("setting root value %s", newRoot.DebugString(ctx, true))

	sessionState := sess.DbStates[dbName]

	h, err := newRoot.HashOf()
	if err != nil {
		return err
	}

	hashStr := h.String()
	err = sess.Session.SetSessionVariable(ctx, WorkingKey(dbName), hashStr)
	if err != nil {
		return err
	}

	sessionState.WorkingSet = sessionState.WorkingSet.WithWorkingRoot(newRoot)

	err = sessionState.EditSession.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	sessionState.dirty = true
	return nil
}

// SetRoots sets new roots for the session for the database named. Typically clients should only set the working root,
// via setRoot. This method is for clients that need to update more of the session state, such as the dolt_ functions.
// Unlike setting the only the working root, this method always marks the database state dirty.
func (sess *Session) SetRoots(ctx *sql.Context, dbName string, roots doltdb.Roots) error {
	// TODO: handle HEAD here?
	workingSet := sess.DbStates[dbName].WorkingSet.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged)
	return sess.SetWorkingSet(ctx, dbName, workingSet, nil)
}

// SetWorkingSet sets the working set for this session.  Unlike setting the working root alone, this method always
// marks the session dirty.
// |headRoot| will be set to the working sets's corresponding HEAD if nil
func (sess *Session) SetWorkingSet(
	ctx *sql.Context,
	dbName string,
	ws *doltdb.WorkingSet,
	headRoot *doltdb.RootValue,
) error {
	if ws == nil {
		panic("attempted to set a nil working set for the session")
	}

	sessionState := sess.DbStates[dbName]
	sessionState.WorkingSet = ws

	if headRoot == nil && !sessionState.detachedHead {
		cs, err := doltdb.NewCommitSpec(ws.Ref().GetPath())
		if err != nil {
			return err
		}

		branchRef, err := ws.Ref().ToHeadRef()
		if err != nil {
			return err
		}

		cm, err := sessionState.dbData.Ddb.Resolve(ctx, cs, branchRef)
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

// SwitchWorkingSet switches to a new working set for this session. Unlike SetWorkingSet, this method expresses no
// intention to eventually persist any uncommitted changes. Rather, this method only changes the in memory state of
// this session. It's equivalent to starting a new session with the working set reference provided. If the current
// session is dirty, this method returns an error. Clients can only switch branches with a clean working set, and so
// must either commit or rollback any changes before attempting to switch working sets.
func (sess *Session) SwitchWorkingSet(
	ctx *sql.Context,
	dbName string,
	wsRef ref.WorkingSetRef) error {
	sessionState := sess.DbStates[dbName]

	if sessionState.dirty {
		return fmt.Errorf("Cannot switch working set, session state is dirty. " +
			"Rollback or commit changes before changing working sets.")
	}

	ws, err := sessionState.dbData.Ddb.ResolveWorkingSet(ctx, wsRef)
	if err == doltdb.ErrWorkingSetNotFound {
		// no working set for this HEAD yet
		ws, err = sess.newWorkingSetForHead(ctx, wsRef, dbName)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// TODO: just call SetWorkingSet?
	sessionState.WorkingSet = ws

	cs, err := doltdb.NewCommitSpec(ws.Ref().GetPath())
	if err != nil {
		return err
	}

	branchRef, err := ws.Ref().ToHeadRef()
	if err != nil {
		return err
	}

	cm, err := sessionState.dbData.Ddb.Resolve(ctx, cs, branchRef)
	if err != nil {
		return err
	}

	sessionState.headCommit = cm
	sessionState.headRoot, err = cm.GetRootValue()
	if err != nil {
		return err
	}

	err = sess.setSessionVarsForDb(ctx, dbName)
	if err != nil {
		return err
	}

	// setRoot updates any edit sessions in use
	err = sess.setRoot(ctx, dbName, ws.WorkingRoot())
	if err != nil {
		return nil
	}

	// After switching to a new working set, we are by definition clean
	sessionState.dirty = false

	// the current transaction, if there is one, needs to be restarted
	ctx.SetTransaction(NewDoltTransaction(ws, wsRef, sessionState.dbData))

	return nil
}

func (sess *Session) WorkingSet(ctx *sql.Context, dbName string) *doltdb.WorkingSet {
	sessionState := sess.DbStates[dbName]
	return sessionState.WorkingSet
}

func (sess *Session) GetTempTableRootValue(ctx *sql.Context, dbName string) (*doltdb.RootValue, bool) {
	dbstate, ok := sess.DbStates[dbName]
	if !ok {
		return nil, false
	}

	if dbstate.TempTableRoot == nil {
		return nil, false
	}

	return dbstate.TempTableRoot, true
}

func (sess *Session) SetTempTableRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sess.DbStates[dbName].TempTableRoot = newRoot
	return sess.DbStates[dbName].TempTableEditSession.SetRoot(ctx, newRoot)
}

// GetHeadCommit returns the parent commit of the current session.
func (sess *Session) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, error) {
	dbState, dbFound := sess.DbStates[dbName]
	if !dbFound {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return dbState.headCommit, nil
}

// SetSessionVariable is defined on sql.Session. We intercept it here to interpret the special semantics of the system
// vars that we define. Otherwise we pass it on to the base implementation.
func (sess *Session) SetSessionVariable(ctx *sql.Context, key string, value interface{}) error {
	// TODO: working set ref

	if isHead, dbName := IsHeadKey(key); isHead {
		err := sess.setHeadSessionVar(ctx, value, dbName)
		if err != nil {
			return err
		}

		sess.DbStates[dbName].detachedHead = true
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

func (sess *Session) setForeignKeyChecksSessionVar(ctx *sql.Context, key string, value interface{}) error {
	convertedVal, err := sql.Int64.Convert(value)
	if err != nil {
		return err
	}
	intVal := int64(0)
	if convertedVal != nil {
		intVal = convertedVal.(int64)
	}
	if intVal == 0 {
		for _, dbState := range sess.DbStates {
			dbState.EditSession.Props.ForeignKeyChecksDisabled = true
		}
	} else if intVal == 1 {
		for _, dbState := range sess.DbStates {
			dbState.EditSession.Props.ForeignKeyChecksDisabled = false
		}
	} else {
		return fmt.Errorf("variable 'foreign_key_checks' can't be set to the value of '%d'", intVal)
	}

	return sess.Session.SetSessionVariable(ctx, key, value)
}

func (sess *Session) setWorkingSessionVar(ctx *sql.Context, value interface{}, dbName string) error {
	valStr, isStr := value.(string) // valStr represents a root val hash
	if !isStr || !hash.IsValid(valStr) {
		return doltdb.ErrInvalidHash
	}

	dbstate, dbFound := sess.DbStates[dbName]
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

func (sess *Session) setHeadSessionVar(ctx *sql.Context, value interface{}, dbName string) error {
	dbstate, dbFound := sess.DbStates[dbName]
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
func (sess *Session) SetSessionVarDirectly(ctx *sql.Context, key string, value interface{}) error {
	return sess.Session.SetSessionVariable(ctx, key, value)
}

// AddDB adds the database given to this session. This establishes a starting root value for this session, as well as
// other state tracking metadata.
func (sess *Session) AddDB(ctx *sql.Context, dbState InitialDbState) error {
	db := dbState.Db
	defineSystemVariables(db.Name())

	sessionState := &DatabaseSessionState{}
	sess.DbStates[db.Name()] = sessionState

	// TODO: get rid of all repo state reader / writer stuff. Until we do, swap out the reader with one of our own, and
	//  the writer with one that errors out
	sessionState.dbData = dbState.DbData
	adapter := NewSessionStateAdapter(sess, db.Name())
	sessionState.dbData.Rsr = adapter
	sessionState.dbData.Rsw = adapter
	sessionState.GlobalState = dbState.GlobalState

	sessionState.EditSession = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})

	err := sess.Session.SetSessionVariable(ctx, HeadRefKey(db.Name()), dbState.WorkingSet.Ref().GetPath())
	if err != nil {
		return err
	}

	sessionState.WorkingSet = dbState.WorkingSet
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
func (sess *Session) CreateTemporaryTablesRoot(ctx *sql.Context, dbName string, ddb *doltdb.DoltDB) error {
	newRoot, err := doltdb.EmptyRootValue(ctx, ddb.ValueReadWriter())
	if err != nil {
		return err
	}

	sess.DbStates[dbName].TempTableEditSession = editor.CreateTableEditSession(newRoot, editor.TableEditSessionProps{})

	return sess.SetTempTableRoot(ctx, dbName, newRoot)
}

// CWBHeadRef returns the branch ref for this session HEAD for the database named
func (sess *Session) CWBHeadRef(dbName string) (ref.DoltRef, error) {
	return sess.DbStates[dbName].WorkingSet.Ref().ToHeadRef()
}

// setSessionVarsForDb updates the three session vars that track the value of the session root hashes
func (sess *Session) setSessionVarsForDb(ctx *sql.Context, dbName string) error {
	state := sess.DbStates[dbName]
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
