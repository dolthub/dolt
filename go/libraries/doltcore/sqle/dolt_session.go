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

type batchMode int8

const (
	single batchMode = iota
	batched
)

const TransactionsEnabledSysVar = "dolt_transactions_enabled"

func init() {
	txEnabledSessionVar := int8(0)
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

	dbRevisions RevisionDatabaseProvider
	dbStates    map[string]*DatabaseSessionState
}

type DatabaseSessionState struct {
	dbName               string
	headCommit           *doltdb.Commit
	detachedHead         bool
	headRoot             *doltdb.RootValue
	workingRoot          *doltdb.RootValue
	workingRef           ref.WorkingSetRef
	dbData               env.DbData
	editSession          *editor.TableEditSession
	dirty                bool
	tempTableRoot        *doltdb.RootValue
	tempTableEditSession *editor.TableEditSession
}

var _ sql.Session = &DoltSession{}

// DefaultDoltSession creates a DoltSession object with default values
func DefaultDoltSession() *DoltSession {
	sess := &DoltSession{
		Session:     sql.NewBaseSession(),
		Username:    "",
		Email:       "",
		dbRevisions: NewDoltDatabaseProvider(),
		dbStates:    make(map[string]*DatabaseSessionState),
	}
	return sess
}

type InitialDbState struct {
	Db          sql.Database
	HeadCommit  *doltdb.Commit
	WorkingRoot *doltdb.RootValue
	DbData      env.DbData
}

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(ctx *sql.Context, sqlSess sql.Session, username, email string, pro RevisionDatabaseProvider, dbs ...InitialDbState) (*DoltSession, error) {
	sess := &DoltSession{
		Session:     sqlSess,
		Username:    username,
		Email:       email,
		dbRevisions: pro,
		dbStates:    make(map[string]*DatabaseSessionState),
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

func (sess *DoltSession) lookupDbState(ctx *sql.Context, dbName string) (*DatabaseSessionState, error) {
	if s, ok := sess.dbStates[dbName]; ok {
		return s, nil
	}

	// attempt to resolve lookup with RevisionDatabaseProvider
	_, init, ok := sess.dbRevisions.DatabaseAtRevision(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	if err := sess.AddDB(ctx, init); err != nil {
		return nil, err
	}

	s, ok := sess.dbStates[dbName]
	if !ok {
		// unreachable if |sess.AddDB()| succeeds
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return s, nil
}

// Flush flushes all changes sitting in edit sessions to the session root for the database named. This normally
// happens automatically as part of statement execution, and is only necessary when the session is manually batched (as
// for bulk SQL import)
func (sess *DoltSession) Flush(ctx *sql.Context, dbName string) error {
	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	editSession := dbState.editSession
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

	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}
	if !dbState.dirty {
		return nil
	}

	// This is triggered when certain commands are sent to the server (ex. commit) when a database is not selected.
	// These commands should not error.
	if dbName == "" {
		return nil
	}

	dbstate, err := sess.lookupDbState(ctx, dbName)
	// It's possible that this returns false if the user has created an in-Memory database. Moreover,
	// the analyzer will check for us whether a db exists or not.
	// TODO: fix this
	if err != nil {
		return err
	}

	// Old "commit" path, which just writes whatever the root for this session is to the repo state file with no care
	// for concurrency. Over time we will disable this path.
	if !TransactionsEnabled(ctx) {
		h, err := dbState.dbData.Ddb.WriteRootValue(ctx, dbState.workingRoot)
		if err != nil {
			return err
		}

		dbstate.dirty = false
		return dbState.dbData.Rsw.SetWorkingHash(ctx, h)
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

	mergedWorkingSet, err := dtx.Commit(ctx, dbstate.workingRoot)
	if err != nil {
		return err
	}

	err = sess.SetRoot(ctx, dbName, mergedWorkingSet)
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
	if !TransactionsEnabled(ctx) || dbName == "" {
		return nil
	}

	dbState, _ := sess.lookupDbState(ctx, dbName)
	if !dbState.dirty {
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

	dbState.dirty = false
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

	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	dtx.CreateSavepoint(savepointName, dbState.workingRoot)
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
func (sess *DoltSession) GetDoltDB(ctx *sql.Context, dbName string) (*doltdb.DoltDB, error) {
	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}

	return dbState.dbData.Ddb, nil
}

func (sess *DoltSession) GetDbData(ctx *sql.Context, dbName string) (env.DbData, error) {
	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return env.DbData{}, err
	}

	return dbState.dbData, nil
}

// GetRoot returns the current working *RootValue for a given database associated with the session
func (sess *DoltSession) GetRoot(ctx *sql.Context, dbName string) (*doltdb.RootValue, error) {
	dbstate, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}

	return dbstate.workingRoot, nil
}

// SetRoot sets a new root value for the session for the database named. This is the primary mechanism by which data
// changes are communicated to the engine and persisted back to disk. All data changes should be followed by a call to
// update the session's root value via this method.
// Data changes contained in the |newRoot| aren't persisted until this session is committed.
// TODO: rename to SetWorkingRoot
func (sess *DoltSession) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	dbState, _ := sess.lookupDbState(ctx, dbName)
	if rootsEqual(dbState.workingRoot, newRoot) {
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

	dbState.workingRoot = newRoot

	err = dbState.editSession.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	dbState.dirty = true
	return nil
}

func (sess *DoltSession) SetTempTableRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	dbState, _ := sess.lookupDbState(ctx, dbName)
	dbState.tempTableRoot = newRoot
	return dbState.tempTableEditSession.SetRoot(ctx, newRoot)
}

// GetHeadCommit returns the parent commit of the current session.
func (sess *DoltSession) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, error) {
	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
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
		dbState, _ := sess.lookupDbState(ctx, dbName)
		dbState.detachedHead = true
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

	dbstate, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return err
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
	dbstate, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return err
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

	ddb := dbState.DbData.Ddb
	if TransactionsEnabled(ctx) {
		if _, ok := dbState.Db.(sql.ReadOnlyDatabase); !ok {
			// Not all dolt commands update the working set ref yet. So until that's true, we update it here with the contents
			// of the repo_state.json file
			headRef := dbState.DbData.Rsr.CWBHeadRef()
			workingSetRef, err := ref.WorkingSetRefForHead(headRef)
			if err != nil {
				return err
			}
			sessionState.workingRef = workingSetRef

			prevHash := hash.Hash{}
			workingSet, err := ddb.ResolveWorkingSet(ctx, workingSetRef)
			if err == doltdb.ErrWorkingSetNotFound {
				// no working set ref established yet
			} else if err != nil {
				return err
			} else {
				prevHash, err = workingSet.Struct().Hash(ddb.Format())
				if err != nil {
					return err
				}
			}

			// TODO: there's a race here if more than one client connects at the same time. We need a retry
			err = ddb.UpdateWorkingSet(ctx, workingSetRef, dbState.WorkingRoot, prevHash)
			if err != nil {
				return err
			}

			err = sess.Session.SetSessionVariable(ctx, HeadRefKey(db.Name()), workingSetRef.GetPath())
			if err != nil {
				return err
			}
		}
	}

	err := sess.SetRoot(ctx, db.Name(), dbState.WorkingRoot)
	if err != nil {
		return err
	}

	logrus.Tracef("working root intialized to %s", dbState.WorkingRoot.DebugString(ctx, false))

	err = sess.SetRoot(ctx, db.Name(), dbState.WorkingRoot)
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
func (sess *DoltSession) CreateTemporaryTablesRoot(ctx *sql.Context, dbName string, ddb *doltdb.DoltDB) (*doltdb.RootValue, error) {
	newRoot, err := doltdb.EmptyRootValue(ctx, ddb.ValueReadWriter())
	if err != nil {
		return nil, err
	}

	dbState, err := sess.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}

	dbState.tempTableEditSession = editor.CreateTableEditSession(newRoot, editor.TableEditSessionProps{})
	err = sess.SetTempTableRoot(ctx, dbName, newRoot)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
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
