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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

type dbRoot struct {
	hashStr string
	root    *doltdb.RootValue
}

const (
	HeadKeySuffix    = "_head"
	HeadRefKeySuffix = "_head_ref"
	WorkingKeySuffix = "_working"
)

const (
	EnableTransactionsEnvKey      = "DOLT_ENABLE_TRANSACTIONS"
	DoltCommitOnTransactionCommit = "dolt_transaction_commit"
)

// TransactionsEnabled controls whether to use SQL transactions
// Exported only for testing
var TransactionsEnabled = false

func init() {
	enableTx, ok := os.LookupEnv(EnableTransactionsEnvKey)
	if ok {
		if strings.ToLower(enableTx) == "true" {
			TransactionsEnabled = true
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
	})
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
	roots                 map[string]dbRoot
	workingSets           map[string]ref.WorkingSetRef
	dbDatas               map[string]env.DbData
	editSessions          map[string]*editor.TableEditSession
	dirty                 map[string]bool
	Username              string
	Email                 string
	tempTableRoots        map[string]*doltdb.RootValue
	tempTableEditSessions map[string]*editor.TableEditSession
}

var _ sql.Session = &DoltSession{}

// DefaultDoltSession creates a DoltSession object with default values
func DefaultDoltSession() *DoltSession {
	sess := &DoltSession{
		Session:               sql.NewBaseSession(),
		roots:                 make(map[string]dbRoot),
		dbDatas:               make(map[string]env.DbData),
		editSessions:          make(map[string]*editor.TableEditSession),
		dirty:                 make(map[string]bool),
		workingSets:           make(map[string]ref.WorkingSetRef),
		Username:              "",
		Email:                 "",
		tempTableRoots:        make(map[string]*doltdb.RootValue),
		tempTableEditSessions: make(map[string]*editor.TableEditSession),
	}
	return sess
}

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(ctx *sql.Context, sqlSess sql.Session, username, email string, dbs ...Database) (*DoltSession, error) {
	dbDatas := make(map[string]env.DbData)
	editSessions := make(map[string]*editor.TableEditSession)

	for _, db := range dbs {
		dbDatas[db.Name()] = env.DbData{Rsw: db.rsw, Ddb: db.ddb, Rsr: db.rsr, Drw: db.drw}
		editSessions[db.Name()] = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})
	}

	sess := &DoltSession{
		Session:               sqlSess,
		dbDatas:               dbDatas,
		editSessions:          editSessions,
		dirty:                 make(map[string]bool),
		roots:                 make(map[string]dbRoot),
		workingSets:           make(map[string]ref.WorkingSetRef),
		Username:              username,
		Email:                 email,
		tempTableRoots:        make(map[string]*doltdb.RootValue),
		tempTableEditSessions: make(map[string]*editor.TableEditSession),
	}
	for _, db := range dbs {
		err := sess.AddDB(ctx, db, db.DbData())

		if err != nil {
			return nil, err
		}
	}

	return sess, nil
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}

// CommitTransaction commits the in-progress transaction for the database named
func (sess *DoltSession) CommitTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if !sess.dirty[dbName] {
		return nil
	}

	// This is triggered when certain commands are sent to the server (ex. commit) when a database is not selected.
	// These commands should not error.
	if dbName == "" {
		return nil
	}

	dbRoot, ok := sess.roots[dbName]
	// It's possible that this returns false if the user has created an in-Memory database. Moreover,
	// the analyzer will check for us whether a db exists or not.
	if !ok {
		return nil
	}

	// Old "commit" path, which just writes whatever the root for this session is to the repo state file with no care
	// for concurrency. Over time we will disable this path.
	if !TransactionsEnabled {
		dbData := sess.dbDatas[dbName]

		h, err := dbData.Ddb.WriteRootValue(ctx, dbRoot.root)
		if err != nil {
			return err
		}

		sess.dirty[dbName] = false
		return dbData.Rsw.SetWorkingHash(ctx, h)
	}

	// Newer commit path does a concurrent merge of the current root with the one other clients are editing, then
	// updates the session with this new root.
	// TODO: validate that the transaction belongs to the DB named
	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	mergedRoot, err := dtx.Commit(ctx, dbRoot.root)
	if err != nil {
		return err
	}

	err = sess.SetRoot(ctx, dbName, mergedRoot)
	if err != nil {
		return err
	}

	err = sess.CommitWorkingSetToDolt(ctx, dtx.dbData, dbName)
	if err != nil {
		return err
	}

	sess.dirty[dbName] = false
	return nil
}

// CommitWorkingSetToDolt stages the working set and then immediately commits the staged changes. This is a Dolt commit
// rather than a transaction commit. If there are no changes to be staged, then no commit is created.
func (sess *DoltSession) CommitWorkingSetToDolt(ctx *sql.Context, dbData env.DbData, dbName string) error {
	if commitBool, err := sess.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommit); err != nil {
		return err
	} else if commitBool.(int8) == 1 {
		fkChecks, err := sess.Session.GetSessionVariable(ctx, "foreign_key_checks")
		if err != nil {
			return err
		}
		err = actions.StageAllTables(ctx, dbData)
		if err != nil {
			return err
		}
		queryTime := ctx.QueryTime()
		_, err = actions.CommitStaged(ctx, dbData, actions.CommitStagedProps{
			Message:          fmt.Sprintf("Transaction commit at %s", queryTime.UTC().Format("2006-01-02T15:04:05Z")),
			Date:             queryTime,
			AllowEmpty:       false,
			CheckForeignKeys: fkChecks.(int8) == 1,
			Name:             sess.Username,
			Email:            sess.Email,
		})
		if _, ok := err.(actions.NothingStaged); err != nil && !ok {
			return err
		}

		headCommit, err := dbData.Ddb.Resolve(ctx, dbData.Rsr.CWBHeadSpec(), dbData.Rsr.CWBHeadRef())
		if err != nil {
			return err
		}
		headHash, err := headCommit.HashOf()
		if err != nil {
			return err
		}
		err = sess.Session.SetSessionVariable(ctx, HeadKey(dbName), headHash.String())
		if err != nil {
			return err
		}
	}
	return nil
}

// RollbackTransaction rolls the given transaction back
func (sess *DoltSession) RollbackTransaction(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled || dbName == "" {
		return nil
	}

	if !sess.dirty[dbName] {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	err := sess.SetRoot(ctx, dbName, dtx.startRoot)
	if err != nil {
		return err
	}

	sess.dirty[dbName] = false
	return nil
}

// CreateSavepoint creates a new savepoint for this transaction with the name given. A previously created savepoint
// with the same name will be overwritten.
func (sess *DoltSession) CreateSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	dtx.CreateSavepoint(savepointName, sess.roots[dbName].root)
	return nil
}

// RollbackToSavepoint sets this session's root to the one saved in the savepoint name. It's an error if no savepoint
// with that name exists.
func (sess *DoltSession) RollbackToSavepoint(ctx *sql.Context, savepointName, dbName string, tx sql.Transaction) error {
	if !TransactionsEnabled || dbName == "" {
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
	if !TransactionsEnabled || dbName == "" {
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
	d, ok := sess.dbDatas[dbName]

	if !ok {
		return nil, false
	}

	return d.Ddb, true
}

func (sess *DoltSession) GetDoltDBRepoStateWriter(dbName string) (env.RepoStateWriter, bool) {
	d, ok := sess.dbDatas[dbName]

	if !ok {
		return nil, false
	}

	return d.Rsw, true
}

func (sess *DoltSession) GetDoltDBRepoStateReader(dbName string) (env.RepoStateReader, bool) {
	d, ok := sess.dbDatas[dbName]

	if !ok {
		return nil, false
	}

	return d.Rsr, true
}

func (sess *DoltSession) GetDoltDBDocsReadWriter(dbName string) (env.DocsReadWriter, bool) {
	d, ok := sess.dbDatas[dbName]

	if !ok {
		return nil, false
	}

	return d.Drw, true
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

// GetRoot returns the current *RootValue for a given database associated with the session
func (sess *DoltSession) GetRoot(dbName string) (*doltdb.RootValue, bool) {
	dbRoot, ok := sess.roots[dbName]

	if !ok {
		return nil, false
	}

	return dbRoot.root, true
}

// SetRoot sets a new root value for the session for the database named. This is the primary mechanism by which data
// changes are communicated to the engine and persisted back to disk. All data changes should be followed by a call to
// update the session's root value via this method.
// Data changes contained in the |newRoot| aren't persisted until this session is committed.
func (sess *DoltSession) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	if rootsEqual(sess.roots[dbName].root, newRoot) {
		return nil
	}

	h, err := newRoot.HashOf()
	if err != nil {
		return err
	}

	hashStr := h.String()
	err = sess.Session.SetSessionVariable(ctx, WorkingKey(dbName), hashStr)
	if err != nil {
		return err
	}

	sess.roots[dbName] = dbRoot{hashStr, newRoot}

	err = sess.editSessions[dbName].SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	sess.dirty[dbName] = true
	return nil
}

func (sess *DoltSession) GetTempTableRootValue(ctx *sql.Context, dbName string) (*doltdb.RootValue, bool) {
	tempTableRoot, ok := sess.tempTableRoots[dbName]

	if !ok {
		return nil, false
	}

	return tempTableRoot, true
}

func (sess *DoltSession) SetTempTableRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	sess.tempTableRoots[dbName] = newRoot
	return sess.tempTableEditSessions[dbName].SetRoot(ctx, newRoot)
}

// GetHeadCommit returns the parent commit of the current session.
func (sess *DoltSession) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, hash.Hash, error) {
	dbd, dbFound := sess.dbDatas[dbName]

	if !dbFound {
		return nil, hash.Hash{}, sql.ErrDatabaseNotFound.New(dbName)
	}

	value, err := sess.Session.GetSessionVariable(ctx, dbName+HeadKeySuffix)
	if err != nil {
		return nil, hash.Hash{}, err
	}

	valStr, isStr := value.(string)

	if !isStr || !hash.IsValid(valStr) {
		return nil, hash.Hash{}, doltdb.ErrInvalidHash
	}

	h := hash.Parse(valStr)
	cs, err := doltdb.NewCommitSpec(valStr)

	if err != nil {
		return nil, hash.Hash{}, err
	}

	cm, err := dbd.Ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, hash.Hash{}, err
	}

	return cm, h, nil
}

// SetSessionVariable is defined on sql.Session. We intercept it here to interpret the special semantics of the system
// vars that we define. Otherwise we pass it on to the base implementation.
func (sess *DoltSession) SetSessionVariable(ctx *sql.Context, key string, value interface{}) error {
	// TODO: is working head ref

	if isHead, dbName := IsHeadKey(key); isHead {
		return sess.setHeadSessionVar(ctx, key, value, dbName)
	}

	if isWorking, dbName := IsWorkingKey(key); isWorking {
		return sess.setWorkingSessionVar(ctx, value, dbName)
	}

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
		for _, tableEditSession := range sess.editSessions {
			tableEditSession.Props.ForeignKeyChecksDisabled = true
		}
	} else if intVal == 1 {
		for _, tableEditSession := range sess.editSessions {
			tableEditSession.Props.ForeignKeyChecksDisabled = false
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

	// If there's a Root Value that's associated with this hash update dbRoots to include it
	dbd, dbFound := sess.dbDatas[dbName]
	if !dbFound {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	root, err := dbd.Ddb.ReadRootValue(ctx, hash.Parse(valStr))
	if errors.Is(doltdb.ErrNoRootValAtHash, err) {
		return nil
	} else if err != nil {
		return err
	}

	return sess.SetRoot(ctx, dbName, root)
}

func (sess *DoltSession) setHeadSessionVar(ctx *sql.Context, key string, value interface{}, dbName string) error {
	dbd, dbFound := sess.dbDatas[dbName]

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

	cm, err := dbd.Ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return err
	}

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
func (sess *DoltSession) AddDB(ctx *sql.Context, db sql.Database, dbData env.DbData) error {
	defineSystemVariables(db.Name())

	rsr := dbData.Rsr
	ddb := dbData.Ddb

	sess.dbDatas[db.Name()] = dbData
	sess.editSessions[db.Name()] = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})

	cs := rsr.CWBHeadSpec()
	headRef := rsr.CWBHeadRef()

	workingHashInRepoState := rsr.WorkingHash()
	workingHashInWsRef := hash.Hash{}

	// TODO: this resolve isn't necessary in all cases and slows things down
	cm, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}

	headCommitHash, err := cm.HashOf()
	if err != nil {
		return err
	}

	var workingRoot *doltdb.RootValue
	// Get a working root to use for this session. This could come from the an independent working set not associated
	// with any commit, or from the head commit itself in some use cases. Some implementors of RepoStateReader use the
	// current HEAD hash as the working set hash, and in fact they have to -- there's not always an independently
	// addressable root value available, only one persisted as a value in a Commit object.
	if headCommitHash == workingHashInRepoState {
		workingRoot, err = cm.GetRootValue()
		if err != nil {
			return err
		}
	}

	if workingRoot == nil {
		// If the root isn't a head commit value, assume it's a standalone value and look it up
		workingRoot, err = ddb.ReadRootValue(ctx, workingHashInRepoState)
		if err != nil {
			return err
		}
	}

	if TransactionsEnabled {
		// Not all dolt commands update the working set ref yet. So until that's true, we update it here with the contents
		// of the repo_state.json file
		workingSetRef, err := ref.WorkingSetRefForHead(headRef)
		if err != nil {
			return err
		}
		sess.workingSets[db.Name()] = workingSetRef

		workingSet, err := ddb.ResolveWorkingSet(ctx, workingSetRef)
		if err == doltdb.ErrWorkingSetNotFound {
			// no working set ref established yet
		} else if err != nil {
			return err
		} else {
			workingHashInWsRef, err = workingSet.Struct().Hash(ddb.Format())
			if err != nil {
				return err
			}
		}

		// TODO: there's a race here if more than one client connects at the same time. We need a retry
		err = ddb.UpdateWorkingSet(ctx, workingSetRef, workingRoot, workingHashInWsRef)
		if err != nil {
			return err
		}

		err = sess.Session.SetSessionVariable(ctx, HeadRefKey(db.Name()), workingSetRef.GetPath())
		if err != nil {
			return err
		}
	}

	err = sess.SetRoot(ctx, db.Name(), workingRoot)
	if err != nil {
		return err
	}

	err = sess.Session.SetSessionVariable(ctx, HeadKey(db.Name()), headCommitHash.String())
	if err != nil {
		return err
	}

	// After setting the initial root we have no state to commit
	sess.dirty[db.Name()] = false

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

	sess.tempTableRoots[dbName] = newRoot
	sess.tempTableEditSessions[dbName] = editor.CreateTableEditSession(newRoot, editor.TableEditSessionProps{})

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
		})
	}
}
