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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	
	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type batchMode int8

const (
	single batchMode = iota
	Batched
)

var ErrWorkingSetChanges = goerrors.NewKind("Cannot switch working set, session state is dirty. " +
	"Rollback or commit changes before changing working sets.")
var ErrSessionNotPeristable = errors.New("session is not persistable")
var ErrCurrentBranchDeleted = errors.New("current branch has been force deleted. run 'USE <database>/<branch>' to checkout a different branch, or reconnect to the server")

// DoltSession is the sql.Session implementation used by dolt. It is accessible through a *sql.Context instance
type DoltSession struct {
	sql.Session
	batchMode        batchMode
	username         string
	email            string
	dbStates         map[string]*DatabaseSessionState
	provider         DoltDatabaseProvider
	tempTables       map[string][]sql.Table
	globalsConf      config.ReadWriteConfig
	branchController *branch_control.Controller
	mu               *sync.Mutex

	// If non-nil, this will be returned from ValidateSession.
	// Used by sqle/cluster to put a session into a terminal err state.
	validateErr error
}

var _ sql.Session = (*DoltSession)(nil)
var _ sql.PersistableSession = (*DoltSession)(nil)
var _ sql.TransactionSession = (*DoltSession)(nil)
var _ branch_control.Context = (*DoltSession)(nil)

// DefaultSession creates a DoltSession with default values
func DefaultSession(pro DoltDatabaseProvider) *DoltSession {
	return &DoltSession{
		Session:          sql.NewBaseSession(),
		username:         "",
		email:            "",
		dbStates:         make(map[string]*DatabaseSessionState),
		provider:         pro,
		tempTables:       make(map[string][]sql.Table),
		globalsConf:      config.NewMapConfig(make(map[string]string)),
		branchController: branch_control.CreateDefaultController(), // Default sessions are fine with the default controller
		mu:               &sync.Mutex{},
	}
}

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(
	sqlSess *sql.BaseSession,
	pro DoltDatabaseProvider,
	conf config.ReadWriteConfig,
	branchController *branch_control.Controller,
) (*DoltSession, error) {
	username := conf.GetStringOrDefault(env.UserNameKey, "")
	email := conf.GetStringOrDefault(env.UserEmailKey, "")
	globals := config.NewPrefixConfig(conf, env.SqlServerGlobalsPrefix)

	sess := &DoltSession{
		Session:          sqlSess,
		username:         username,
		email:            email,
		dbStates:         make(map[string]*DatabaseSessionState),
		provider:         pro,
		tempTables:       make(map[string][]sql.Table),
		globalsConf:      globals,
		branchController: branchController,
		mu:               &sync.Mutex{},
	}

	return sess, nil
}

// Provider returns the RevisionDatabaseProvider for this session.
func (d *DoltSession) Provider() DoltDatabaseProvider {
	return d.provider
}

// EnableBatchedMode enables batched mode for this session. This is only safe to do during initialization.
// Sessions operating in batched mode don't flush any edit buffers except when told to do so explicitly, or when a
// transaction commits. Disable @@autocommit to prevent edit buffers from being flushed prematurely in this mode.
func (d *DoltSession) EnableBatchedMode() {
	d.batchMode = Batched
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}

// lookupDbState is the private version of LookupDbState, returning a struct that has more information available than
// the interface returned by the public method.
func (d *DoltSession) lookupDbState(ctx *sql.Context, dbName string) (*branchState, bool, error) {
	// TODO: change to require a db, not a name so that the below doesn't need to do a string split
	dbName = strings.ToLower(dbName)
	
	parts := strings.SplitN(dbName, DbRevisionDelimiter, 2)
	baseName := parts[0]

	d.mu.Lock()
	dbState, ok := d.dbStates[baseName]
	d.mu.Unlock()

	if ok {
		_, rev := SplitRevisionDbName(dbState.db)
		branchState, ok := dbState.heads[strings.ToLower(rev)]

		if ok {
			if dbState.Err != nil {
				return nil, false, dbState.Err
			}

			return branchState, ok, nil
		}
	}
	
	// no state for this db / branch combination yet, look it up from the provider
	database, ok, err := d.provider.SessionDatabase(ctx, dbName)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	// Add the initial state to the session for future reuse
	if err = d.addDB(ctx, database); err != nil {
		return nil, false, err
	}

	d.mu.Lock()
	dbState, ok = d.dbStates[dbName]
	d.mu.Unlock()
	if !ok {
		return nil, false, sql.ErrDatabaseNotFound.New(dbName)
	}

	_, rev := SplitRevisionDbName(database)
	return dbState.heads[strings.ToLower(rev)], true, nil
}

// TODO NEXT: the lookupdbstate method is the key abstraction point. It proxies all non-revisoined DBs to a revisioned 
//  state, based on the current db (the default branch if none). The session stores all data by working set ref. DB 
//  names are masked on return in the case of a non-revisined DB. Session stae is also responsible for keeping track of
//  the checked out branch in the case of a default (no branch-specified) db connection.
//  Alternate idea: branch head is always set correctly in response to a USE statement, OR a checkout procedure. The
//  latter implicitly updates the current database. You also get a revision db checked out on connection to no branch.
func (d *DoltSession) LookupDbState(ctx *sql.Context, dbName string) (SessionState, bool, error) {
	s, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, false, err
	}

	return s, ok, nil
}

// RemoveDbState invalidates any cached db state in this session, for example, if a database is dropped.
func (d *DoltSession) RemoveDbState(_ *sql.Context, dbName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.dbStates, strings.ToLower(dbName))
	return nil
}

// Flush flushes all changes sitting in edit sessions to the session root for the database named. This normally
// happens automatically as part of statement execution, and is only necessary when the session is manually batched (as
// for bulk SQL import)
func (d *DoltSession) Flush(ctx *sql.Context, dbName string) error {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	ws, err := branchState.WriteSession().Flush(ctx)
	if err != nil {
		return err
	}

	return d.SetRoot(ctx, dbName, ws.WorkingRoot())
}

// SetValidateErr sets an error on this session to be returned from every call
// to ValidateSession. This is effectively a way to disable a session.
//
// Used by sql/cluster logic to make sessions on a server which has
// transitioned roles termainlly error.
func (d *DoltSession) SetValidateErr(err error) {
	d.validateErr = err
}

// ValidateSession validates a working set if there are a valid sessionState with non-nil working set.
// If there is no sessionState or its current working set not defined, then no need for validation,
// so no error is returned.
func (d *DoltSession) ValidateSession(ctx *sql.Context, dbName string) error {
	if d.validateErr != nil {
		return d.validateErr
	}
	sessionState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if sessionState.WorkingSet() == nil {
		return nil
	}
	wsRef := sessionState.WorkingSet().Ref()
	_, err = sessionState.dbData.Ddb.ResolveWorkingSet(ctx, wsRef)
	if err == doltdb.ErrWorkingSetNotFound {
		_, err = d.newWorkingSetForHead(ctx, wsRef, dbName)
		// if the current head is not found, the branch was force deleted, so use nil working set.
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			return ErrCurrentBranchDeleted
		} else if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// StartTransaction refreshes the state of this session and starts a new transaction.
func (d *DoltSession) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	// New transaction, clear all session state
	// TODO: revisit this
	d.clearRevisionDbState()

	// Take a snapshot of the current noms root for every database under management
	doltDatabases := d.provider.DoltDatabases()
	txDbs := make([]SqlDatabase, 0, len(doltDatabases))
	for _, db := range doltDatabases {
		// TODO: this nil check is only necessary to support UserSpaceDatabase, come up with a better set of interfaces 
		//  to capture these capabilities
		ddb := db.DbData().Ddb
		if ddb != nil {
			rrd, ok := db.(RemoteReadReplicaDatabase)
			if ok && rrd.ValidReplicaState(ctx) {
				err := rrd.PullFromRemote(ctx)
				if err != nil && !IgnoreReplicationErrors() {
					return nil, fmt.Errorf("replication error: %w", err)
				} else if err != nil {
					WarnReplicationError(ctx, err)
				}
			}

			if _, v, ok := sql.SystemVariables.GetGlobal(ReadReplicaRemote); ok && v != "" {
				err := ddb.Rebase(ctx)
				if err != nil && !IgnoreReplicationErrors() {
					return nil, err
				} else if err != nil {
					WarnReplicationError(ctx, err)
				}
			}

			txDbs = append(txDbs, db)
		}
	}
	
	return NewMultiHeadTransaction(ctx, txDbs, tCharacteristic)
}

// clearRevisionDbState clears all revision DB states for this session. This is necessary on transaction start,
// because they will be re-initialized with the current branch head / working set.
// TODO: this should happen with every dbstate, not just revision DBs. The problem is that we track the current working
//
//	set *only* in the session state. We need to disentangle the metadata about a state (working ref, persists across
//	transactions) from its data (re-initialized on every transaction start)
func (d *DoltSession) clearRevisionDbState() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, dbState := range d.dbStates {
		if len(dbState.db.Revision()) > 0 {
			delete(d.dbStates, strings.ToLower(dbState.db.Name()))
		}
	}
}

// isNoOpTransactionDatabase returns whether the database name given is a non-Dolt database that shouldn't have
// transaction logic performed on it
func isNoOpTransactionDatabase(dbName string) bool {
	return len(dbName) == 0 || dbName == "information_schema" || dbName == "mysql"
}

func (d *DoltSession) newWorkingSetForHead(ctx *sql.Context, wsRef ref.WorkingSetRef, dbName string) (*doltdb.WorkingSet, error) {
	dbData, _ := d.GetDbData(nil, dbName)

	headSpec, _ := doltdb.NewCommitSpec("HEAD")
	headRef, err := wsRef.ToHeadRef()
	if err != nil {
		return nil, err
	}

	headCommit, err := dbData.Ddb.Resolve(ctx, headSpec, headRef)
	if err != nil {
		return nil, err
	}

	headRoot, err := headCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	return doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(headRoot).WithStagedRoot(headRoot), nil
}

// CommitTransaction commits the in-progress transaction for the database named. Depending on session settings, this
// may write only a new working set, or may additionally create a new dolt commit for the current HEAD.
func (d *DoltSession) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	dbName := ctx.GetTransactionDatabase()
	if isNoOpTransactionDatabase(dbName) {
		return nil
	}

	if d.BatchMode() == Batched {
		err := d.Flush(ctx, dbName)
		if err != nil {
			return err
		}
	}

	if TransactionsDisabled(ctx) {
		return nil
	}

	// This is triggered when certain commands are sent to the server (ex. commit) when a database is not selected.
	// These commands should not error.
	if dbName == "" {
		return nil
	}

	performDoltCommitVar, err := d.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommit)
	if err != nil {
		return err
	}

	peformDoltCommitInt, ok := performDoltCommitVar.(int8)
	if !ok {
		return fmt.Errorf(fmt.Sprintf("Unexpected type for var %s: %T", DoltCommitOnTransactionCommit, performDoltCommitVar))
	}

	if peformDoltCommitInt == 1 {
		pendingCommit, err := d.PendingCommitAllStaged(ctx, dbName, actions.CommitStagedProps{
			Message:    "Transaction commit",
			Date:       ctx.QueryTime(),
			AllowEmpty: false,
			Force:      false,
			Name:       d.Username(),
			Email:      d.Email(),
		})
		if err != nil {
			return err
		}

		// Nothing to stage, so fall back to CommitWorkingSet logic instead
		if pendingCommit == nil {
			return d.CommitWorkingSet(ctx, dbName, tx)
		}

		_, err = d.DoltCommit(ctx, dbName, tx, pendingCommit)
		return err
	} else {
		return d.CommitWorkingSet(ctx, dbName, tx)
	}
}

// isDirty returns whether the working set for the database named is dirty
// TODO: remove the dbname parameter, return a global dirty bit
// TODO: re-evaluate dirty tracking like this altogether, use tx data
func (d *DoltSession) isDirty(ctx *sql.Context, dbName string) (bool, error) {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return false, err
	}
	
	return branchState.dbState.dirty, nil
}

// CommitWorkingSet commits the working set for the transaction given, without creating a new dolt commit.
// Clients should typically use CommitTransaction, which performs additional checks, instead of this method.
func (d *DoltSession) CommitWorkingSet(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	commitFunc := func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error) {
		ws, err := dtx.Commit(ctx, workingSet, dbName)
		return ws, nil, err
	}

	_, err := d.doCommit(ctx, dbName, tx, commitFunc)
	return err
}

// DoltCommit commits the working set and a new dolt commit with the properties given.
// Clients should typically use CommitTransaction, which performs additional checks, instead of this method.
func (d *DoltSession) DoltCommit(
	ctx *sql.Context,
	dbName string,
	tx sql.Transaction,
	commit *doltdb.PendingCommit,
) (*doltdb.Commit, error) {
	commitFunc := func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error) {
		ws, commit, err := dtx.DoltCommit(
			ctx,
			workingSet.WithWorkingRoot(commit.Roots.Working).WithStagedRoot(commit.Roots.Staged),
			commit)
		if err != nil {
			return nil, nil, err
		}

		// Unlike normal COMMIT statements, CALL DOLT_COMMIT() doesn't get the current transaction cleared out by the query
		// engine, so we do it here.
		// TODO: the engine needs to manage this
		ctx.SetTransaction(nil)

		return ws, commit, err
	}

	return d.doCommit(ctx, dbName, tx, commitFunc)
}

// doCommitFunc is a function to write to the database, which involves updating the working set and potentially
// updating HEAD with a new commit
type doCommitFunc func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error)

// doCommit exercise the business logic for a particular doCommitFunc
func (d *DoltSession) doCommit(ctx *sql.Context, dbName string, tx sql.Transaction, commitFunc doCommitFunc) (*doltdb.Commit, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	} else if !ok {
		// It's possible that we don't have dbstate if the user has created an in-Memory database. Moreover,
		// the analyzer will check for us whether a db exists or not.
		// TODO: fix this
		return nil, nil
	}

	// TODO: validate that the transaction belongs to the DB named
	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return nil, fmt.Errorf("expected a DoltTransaction")
	}

	mergedWorkingSet, newCommit, err := commitFunc(ctx, dtx, branchState.WorkingSet())
	if err != nil {
		return nil, err
	}

	err = d.SetWorkingSet(ctx, dbName, mergedWorkingSet)
	if err != nil {
		return nil, err
	}

	branchState.dbState.dirty = false
	return newCommit, nil
}

// PendingCommitAllStaged returns a pending commit with all tables staged. Returns nil if there are no changes to stage.
func (d *DoltSession) PendingCommitAllStaged(ctx *sql.Context, dbName string, props actions.CommitStagedProps) (*doltdb.PendingCommit, error) {
	roots, ok := d.GetRoots(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("Couldn't get info for database %s", dbName)
	}

	var err error
	roots, err = actions.StageAllTables(ctx, roots, true)
	if err != nil {
		return nil, err
	}

	return d.NewPendingCommit(ctx, dbName, roots, props)
}

// NewPendingCommit returns a new |doltdb.PendingCommit| for the database named, using the roots given, adding any
// merge parent from an in progress merge as appropriate. The session working set is not updated with these new roots,
// but they are set in the returned |doltdb.PendingCommit|. If there are no changes staged, this method returns nil.
func (d *DoltSession) NewPendingCommit(ctx *sql.Context, dbName string, roots doltdb.Roots, props actions.CommitStagedProps) (*doltdb.PendingCommit, error) {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}

	headCommit := branchState.headCommit
	headHash, _ := headCommit.HashOf()

	var mergeParentCommits []*doltdb.Commit
	if branchState.WorkingSet().MergeActive() {
		mergeParentCommits = []*doltdb.Commit{branchState.WorkingSet().MergeState().Commit()}
	} else if props.Amend {
		numParentsHeadForAmend := headCommit.NumParents()
		for i := 0; i < numParentsHeadForAmend; i++ {
			parentCommit, err := headCommit.GetParent(ctx, i)
			if err != nil {
				return nil, err
			}
			mergeParentCommits = append(mergeParentCommits, parentCommit)
		}

		// TODO: This is not the correct way to write this commit as an amend. While this commit is running
		//  the branch head moves backwards and concurrency control here is not principled.
		newRoots, err := actions.ResetSoftToRef(ctx, branchState.dbData, "HEAD~1")
		if err != nil {
			return nil, err
		}

		err = d.SetWorkingSet(ctx, dbName, branchState.WorkingSet().WithStagedRoot(newRoots.Staged))
		if err != nil {
			return nil, err
		}

		roots.Head = newRoots.Head
	}

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, branchState.WorkingSet(), mergeParentCommits, branchState.dbData.Ddb, props)
	if err != nil {
		if props.Amend {
			_, err = actions.ResetSoftToRef(ctx, branchState.dbData, headHash.String())
			if err != nil {
				return nil, err
			}
		}
		if _, ok := err.(actions.NothingStaged); err != nil && !ok {
			return nil, err
		}
	}

	return pendingCommit, nil
}

// Rollback rolls the given transaction back
func (d *DoltSession) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	// Nothing to do here, we just throw away all our work and let a new transaction begin next statement
	d.clearRevisionDbState()
	return nil
}

// CreateSavepoint creates a new savepoint for this transaction with the name given. A previously created savepoint
// with the same name will be overwritten.
func (d *DoltSession) CreateSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	dbName := ctx.GetTransactionDatabase()

	if TransactionsDisabled(ctx) || dbName == "" {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	dtx.CreateSavepoint(savepointName, branchState.roots().Working)
	return nil
}

// RollbackToSavepoint sets this session's root to the one saved in the savepoint name. It's an error if no savepoint
// with that name exists.
func (d *DoltSession) RollbackToSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	dbName := ctx.GetTransactionDatabase()

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

	err := d.SetRoot(ctx, dbName, root)
	if err != nil {
		return err
	}

	return nil
}

// ReleaseSavepoint removes the savepoint name from the transaction. It's an error if no savepoint with that name
// exists.
func (d *DoltSession) ReleaseSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	dbName := ctx.GetTransactionDatabase()

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
func (d *DoltSession) GetDoltDB(ctx *sql.Context, dbName string) (*doltdb.DoltDB, bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, false
	}
	if !ok {
		return nil, false
	}

	return branchState.dbData.Ddb, true
}

func (d *DoltSession) GetDbData(ctx *sql.Context, dbName string) (env.DbData, bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return env.DbData{}, false
	}
	if !ok {
		return env.DbData{}, false
	}

	return branchState.dbData, true
}

// GetRoots returns the current roots for a given database associated with the session
func (d *DoltSession) GetRoots(ctx *sql.Context, dbName string) (doltdb.Roots, bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return doltdb.Roots{}, false
	}
	if !ok {
		return doltdb.Roots{}, false
	}

	return branchState.roots(), true
}

// ResolveRootForRef returns the root value for the ref given, which refers to either a commit spec or is one of the
// special identifiers |WORKING| or |STAGED|
// Returns the root value associated with the identifier given, its commit time and its hash string. The hash string
// for special identifiers |WORKING| or |STAGED| would be itself, 'WORKING' or 'STAGED', respectively.
func (d *DoltSession) ResolveRootForRef(ctx *sql.Context, dbName, refStr string) (*doltdb.RootValue, *types.Timestamp, string, error) {
	if refStr == doltdb.Working || refStr == doltdb.Staged {
		// TODO: get from working set / staged update time
		now := types.Timestamp(time.Now())
		// TODO: no current database
		roots, _ := d.GetRoots(ctx, ctx.GetCurrentDatabase())
		if refStr == doltdb.Working {
			return roots.Working, &now, refStr, nil
		} else if refStr == doltdb.Staged {
			return roots.Staged, &now, refStr, nil
		}
	}

	var root *doltdb.RootValue
	var commitTime *types.Timestamp
	cs, err := doltdb.NewCommitSpec(refStr)
	if err != nil {
		return nil, nil, "", err
	}

	dbData, ok := d.GetDbData(ctx, dbName)
	if !ok {
		return nil, nil, "", sql.ErrDatabaseNotFound.New(dbName)
	}

	headRef, err := d.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, nil, "", err
	}

	cm, err := dbData.Ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, nil, "", err
	}

	root, err = cm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, "", err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, nil, "", err
	}

	t := meta.Time()
	commitTime = (*types.Timestamp)(&t)

	commitHash, err := cm.HashOf()
	if err != nil {
		return nil, nil, "", err
	}

	return root, commitTime, commitHash.String(), nil
}

// SetRoot sets a new root value for the session for the database named. This is the primary mechanism by which data
// changes are communicated to the engine and persisted back to disk. All data changes should be followed by a call to
// update the session's root value via this method.
// Data changes contained in the |newRoot| aren't persisted until this session is committed.
// TODO: rename to SetWorkingRoot
// TODO: kill this method
func (d *DoltSession) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	// TODO: this is redundant with work done in setRoot
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	if rootsEqual(branchState.roots().Working, newRoot) {
		return nil
	}

	if branchState.readOnly {
		// TODO: Return an error here?
		return nil
	}
	branchState.workingSet = branchState.WorkingSet().WithWorkingRoot(newRoot)

	return d.SetWorkingSet(ctx, dbName, branchState.WorkingSet())
}

// SetRoots sets new roots for the session for the database named. Typically clients should only set the working root,
// via setRoot. This method is for clients that need to update more of the session state, such as the dolt_ functions.
// Unlike setting the working root, this method always marks the database state dirty.
func (d *DoltSession) SetRoots(ctx *sql.Context, dbName string, roots doltdb.Roots) error {
	// TODO: handle HEAD here?
	sessionState, _, err := d.LookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	workingSet := sessionState.WorkingSet().WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged)
	return d.SetWorkingSet(ctx, dbName, workingSet)
}

// SetWorkingSet sets the working set for this session.
// Unlike setting the working root alone, this method always marks the session dirty.
func (d *DoltSession) SetWorkingSet(ctx *sql.Context, dbName string, ws *doltdb.WorkingSet) error {
	if ws == nil {
		panic("attempted to set a nil working set for the session")
	}

	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}
	if ws.Ref() != branchState.WorkingSet().Ref() {
		return fmt.Errorf("must switch working sets with SwitchWorkingSet")
	}
	branchState.workingSet = ws

	cs, err := doltdb.NewCommitSpec(ws.Ref().GetPath())
	if err != nil {
		return err
	}

	branchRef, err := ws.Ref().ToHeadRef()
	if err != nil {
		return err
	}

	cm, err := branchState.dbData.Ddb.Resolve(ctx, cs, branchRef)
	if err != nil {
		return err
	}
	branchState.headCommit = cm

	headRoot, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}
	branchState.headRoot = headRoot

	err = d.setSessionVarsForDb(ctx, dbName)
	if err != nil {
		return err
	}

	err = branchState.WriteSession().SetWorkingSet(ctx, ws)
	if err != nil {
		return err
	}

	branchState.dbState.dirty = true

	return nil
}

// SwitchWorkingSet switches to a new working set for this session. Unlike SetWorkingSet, this method expresses no
// intention to eventually persist any uncommitted changes. Rather, this method only changes the in memory state of
// this session. It's equivalent to starting a new session with the working set reference provided. If the current
// session is dirty, this method returns an error. Clients can only switch branches with a clean working set, and so
// must either commit or rollback any changes before attempting to switch working sets.
func (d *DoltSession) SwitchWorkingSet(
	ctx *sql.Context,
	dbName string,
	wsRef ref.WorkingSetRef,
) error {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	// TODO: should this be an error if any database in the transaction is dirty, or just this one?
	if branchState.dbState.dirty {
		return ErrWorkingSetChanges.New()
	}

	// TODO: this should call session.StartTransaction once that has been cleaned up a bit
	nomsRoots := make(map[string]hash.Hash)
	for _, db := range d.provider.DoltDatabases() {
		nomsRoot, err := db.DbData().Ddb.NomsRoot(ctx)
		if err != nil {
			return err
		}
		nomsRoots[strings.ToLower(db.Name())] = nomsRoot
	}

	// TODO: resolve the working set ref with the root above
	ws, err := branchState.dbData.Ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	// TODO: just call SetWorkingSet?
	branchState.workingSet = ws

	cs, err := doltdb.NewCommitSpec(ws.Ref().GetPath())
	if err != nil {
		return err
	}

	branchRef, err := ws.Ref().ToHeadRef()
	if err != nil {
		return err
	}

	cm, err := branchState.dbData.Ddb.Resolve(ctx, cs, branchRef)
	if err != nil {
		return err
	}

	branchState.headCommit = cm
	branchState.headRoot, err = cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	err = d.setSessionVarsForDb(ctx, dbName)
	if err != nil {
		return err
	}

	h, err := ws.WorkingRoot().HashOf()
	if err != nil {
		return err
	}

	err = d.Session.SetSessionVariable(ctx, WorkingKey(dbName), h.String())
	if err != nil {
		return err
	}

	// make a fresh WriteSession, discard existing WriteSession
	opts := branchState.WriteSession().GetOptions()
	nbf := ws.WorkingRoot().VRW().Format()
	tracker, err := branchState.dbState.globalState.GetAutoIncrementTracker(ctx)
	if err != nil {
		return err
	}
	branchState.writeSession = writer.NewWriteSession(nbf, ws, tracker, opts)

	// After switching to a new working set, we are by definition clean
	// TODO: obviously this is no longer true but this entire method needs a rewrite to tolerate writing to multiple
	//  heads in one tx
	branchState.dbState.dirty = false

	// the current transaction, if there is one, needs to be restarted
	tCharacteristic := sql.ReadWrite
	if t := ctx.GetTransaction(); t != nil {
		if t.IsReadOnly() {
			tCharacteristic = sql.ReadOnly
		}
	}
	ctx.SetTransaction(NewDoltTransaction(nomsRoots, ws, wsRef, branchState.dbData, branchState.WriteSession().GetOptions(), tCharacteristic))

	return nil
}

func (d *DoltSession) WorkingSet(ctx *sql.Context, dbName string) (*doltdb.WorkingSet, error) {
	sessionState, _, err := d.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	return sessionState.WorkingSet(), nil
}

// GetHeadCommit returns the parent commit of the current session.
func (d *DoltSession) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return branchState.headCommit, nil
}

// SetSessionVariable is defined on sql.Session. We intercept it here to interpret the special semantics of the system
// vars that we define. Otherwise we pass it on to the base implementation.
func (d *DoltSession) SetSessionVariable(ctx *sql.Context, key string, value interface{}) error {
	if ok, db := IsHeadRefKey(key); ok {
		v, ok := value.(string)
		if !ok {
			return doltdb.ErrInvalidBranchOrHash
		}
		if err := d.setHeadRefSessionVar(ctx, db, v); err != nil {
			return err
		}
	}
	if IsReadOnlyVersionKey(key) {
		return sql.ErrSystemVariableReadOnly.New(key)
	}

	if strings.ToLower(key) == "foreign_key_checks" {
		return d.setForeignKeyChecksSessionVar(ctx, key, value)
	}

	return d.Session.SetSessionVariable(ctx, key, value)
}

func (d *DoltSession) setHeadRefSessionVar(ctx *sql.Context, db, value string) error {
	headRef, err := ref.Parse(value)
	if err != nil {
		return err
	}

	ws, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return err
	}
	err = d.SwitchWorkingSet(ctx, db, ws)
	if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		return fmt.Errorf("%w; %s: '%s'", doltdb.ErrBranchNotFound, err, value)
	}
	return err
}

func (d *DoltSession) setForeignKeyChecksSessionVar(ctx *sql.Context, key string, value interface{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	convertedVal, _, err := sqltypes.Int64.Convert(value)
	if err != nil {
		return err
	}
	intVal := int64(0)
	if convertedVal != nil {
		intVal = convertedVal.(int64)
	}
	
	if intVal == 0 {
		for _, dbState := range d.dbStates {
			for _, branchState := range dbState.heads {
				opts := branchState.WriteSession().GetOptions()
				opts.ForeignKeyChecksDisabled = true
				branchState.WriteSession().SetOptions(opts)
			}
		}
	} else if intVal == 1 {
		for _, dbState := range d.dbStates {
			for _, branchState := range dbState.heads {
				opts := branchState.WriteSession().GetOptions()
				opts.ForeignKeyChecksDisabled = false
				branchState.WriteSession().SetOptions(opts)
			}
		}
	} else {
		return fmt.Errorf("variable 'foreign_key_checks' can't be set to the value of '%d'", intVal)
	}

	return d.Session.SetSessionVariable(ctx, key, value)
}

// HasDB returns true if |sess| is tracking state for this database.
func (d *DoltSession) HasDB(_ *sql.Context, dbName string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.dbStates[strings.ToLower(dbName)]
	return ok
}

// addDB adds the database given to this session. This establishes a starting root value for this session, as well as
// other state tracking metadata.
func (d *DoltSession) addDB(ctx *sql.Context, db SqlDatabase) error {
	DefineSystemVariablesForDB(db.Name())

	var sessionState *DatabaseSessionState
	branchState := &branchState{} 
	baseName, rev := SplitRevisionDbName(db)
	
	dbState, err := db.InitialDBState(ctx, rev)
	if err != nil {
		return err
	}

	d.mu.Lock()
	if _, ok := d.dbStates[strings.ToLower(baseName)]; !ok {
		sessionState = NewEmptyDatabaseSessionState()
		d.dbStates[strings.ToLower(baseName)] = sessionState

		tmpDir, err := dbState.DbData.Rsw.TempTableFilesDir()
		if err != nil {
			if errors.Is(err, env.ErrDoltRepositoryNotFound) {
				return env.ErrFailedToAccessDB.New(dbState.Db.Name())
			}
			return err
		}
		sessionState.tmpFileDir = tmpDir
	}
	branchState.dbState = sessionState
	sessionState.heads[strings.ToLower(rev)] = branchState
	d.mu.Unlock()

	sessionState.dbName = baseName
	// TODO: this doesn't seem right, shouldn't be a revision DB
	sessionState.db = db

	// TODO: get rid of all repo state reader / writer stuff. Until we do, swap out the reader with one of our own, and
	//  the writer with one that errors out
	// TODO: this no longer gets called at session creation time, so the error handling below never occurs when a
	//  database is deleted out from under a running server
	branchState.dbData = dbState.DbData
	adapter := NewSessionStateAdapter(d, db.Name(), dbState.Remotes, dbState.Branches, dbState.Backups)
	branchState.dbData.Rsr = adapter
	branchState.dbData.Rsw = adapter
	branchState.readOnly = dbState.ReadOnly

	// TODO: figure out how to cast this to dsqle.SqlDatabase without creating import cycles
	// Or better yet, get rid of EditOptions from the database, it's a session setting
	nbf := types.Format_Default
	if branchState.dbData.Ddb != nil {
		nbf = branchState.dbData.Ddb.Format()
	}
	editOpts := db.(interface{ EditOptions() editor.Options }).EditOptions()

	if dbState.Err != nil {
		sessionState.Err = dbState.Err
	} else if dbState.WorkingSet != nil {
		branchState.workingSet = dbState.WorkingSet

		// TODO: this is pretty clunky, there is a silly dependency between InitialDbState and globalstate.StateProvider
		//  that's hard to express with the current types
		stateProvider, ok := db.(globalstate.StateProvider)
		if !ok {
			return fmt.Errorf("database does not contain global state store")
		}
		sessionState.globalState = stateProvider.GetGlobalState()

		tracker, err := sessionState.globalState.GetAutoIncrementTracker(ctx)
		if err != nil {
			return err
		}
		branchState.writeSession = writer.NewWriteSession(nbf, branchState.WorkingSet(), tracker, editOpts)
		if err = d.SetWorkingSet(ctx, db.Name(), dbState.WorkingSet); err != nil {
			return err
		}
	} else if dbState.HeadCommit != nil {
		// WorkingSet is nil in the case of a read only, detached head DB
		headRoot, err := dbState.HeadCommit.GetRootValue(ctx)
		if err != nil {
			return err
		}
		branchState.headRoot = headRoot
	} else if dbState.HeadRoot != nil {
		branchState.headRoot = dbState.HeadRoot
	}

	// This has to happen after SetWorkingSet above, since it does a stale check before its work
	// TODO: this needs to be kept up to date as the working set ref changes
	branchState.headCommit = dbState.HeadCommit

	// After setting the initial root we have no state to commit
	// TODO: do we still want to track dirty states, or just look at hashes
	sessionState.dirty = false

	if sessionState.Err == nil {
		return d.setSessionVarsForDb(ctx, db.Name())
	}
	return nil
}

func (d *DoltSession) AddTemporaryTable(ctx *sql.Context, db string, tbl sql.Table) {
	d.tempTables[db] = append(d.tempTables[db], tbl)
}

func (d *DoltSession) DropTemporaryTable(ctx *sql.Context, db, name string) {
	tables := d.tempTables[db]
	for i, tbl := range d.tempTables[db] {
		if strings.ToLower(tbl.Name()) == strings.ToLower(name) {
			tables = append(tables[:i], tables[i+1:]...)
			break
		}
	}
	d.tempTables[db] = tables
}

func (d *DoltSession) GetTemporaryTable(ctx *sql.Context, db, name string) (sql.Table, bool) {
	for _, tbl := range d.tempTables[db] {
		if strings.ToLower(tbl.Name()) == strings.ToLower(name) {
			return tbl, true
		}
	}
	return nil, false
}

// GetAllTemporaryTables returns all temp tables for this session.
func (d *DoltSession) GetAllTemporaryTables(ctx *sql.Context, db string) ([]sql.Table, error) {
	return d.tempTables[db], nil
}

// CWBHeadRef returns the branch ref for this session HEAD for the database named
func (d *DoltSession) CWBHeadRef(ctx *sql.Context, dbName string) (ref.DoltRef, error) {
	dbState, _, err := d.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}

	if dbState.WorkingSet() == nil {
		return nil, nil
	}

	return dbState.WorkingSet().Ref().ToHeadRef()
}

func (d *DoltSession) Username() string {
	return d.username
}

func (d *DoltSession) Email() string {
	return d.email
}

func (d *DoltSession) BatchMode() batchMode {
	return d.batchMode
}

// setSessionVarsForDb updates the three session vars that track the value of the session root hashes
func (d *DoltSession) setSessionVarsForDb(ctx *sql.Context, dbName string) error {
	state, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	// Different DBs have different requirements for what state is set, so we are maximally permissive on what's expected
	// in the state object here
	if state.WorkingSet() != nil {
		headRef, err := state.WorkingSet().Ref().ToHeadRef()
		if err != nil {
			return err
		}

		err = d.Session.SetSessionVariable(ctx, HeadRefKey(dbName), headRef.String())
		if err != nil {
			return err
		}
	}

	roots := state.roots()

	if roots.Working != nil {
		h, err := roots.Working.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, WorkingKey(dbName), h.String())
		if err != nil {
			return err
		}
	}

	if roots.Staged != nil {
		h, err := roots.Staged.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, StagedKey(dbName), h.String())
		if err != nil {
			return err
		}
	}

	if state.headCommit != nil {
		h, err := state.headCommit.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, HeadKey(dbName), h.String())
		if err != nil {
			return err
		}
	}

	return nil
}

func (d DoltSession) WithGlobals(conf config.ReadWriteConfig) *DoltSession {
	d.globalsConf = conf
	return &d
}

// PersistGlobal implements sql.PersistableSession
func (d *DoltSession) PersistGlobal(sysVarName string, value interface{}) error {
	if d.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return setPersistedValue(d.globalsConf, sysVar.Name, value)
}

// RemovePersistedGlobal implements sql.PersistableSession
func (d *DoltSession) RemovePersistedGlobal(sysVarName string) error {
	if d.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.globalsConf.Unset([]string{sysVar.Name})
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (d *DoltSession) RemoveAllPersistedGlobals() error {
	if d.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	allVars := make([]string, d.globalsConf.Size())
	i := 0
	d.globalsConf.Iter(func(k, v string) bool {
		allVars[i] = k
		i++
		return false
	})

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.globalsConf.Unset(allVars)
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (d *DoltSession) GetPersistedValue(k string) (interface{}, error) {
	if d.globalsConf == nil {
		return nil, ErrSessionNotPeristable
	}

	return getPersistedValue(d.globalsConf, k)
}

// SystemVariablesInConfig returns a list of System Variables associated with the session
func (d *DoltSession) SystemVariablesInConfig() ([]sql.SystemVariable, error) {
	if d.globalsConf == nil {
		return nil, ErrSessionNotPeristable
	}
	sysVars, _, err := SystemVariablesInConfig(d.globalsConf)
	if err != nil {
		return nil, err
	}
	return sysVars, nil
}

// GetBranch implements the interface branch_control.Context.
func (d *DoltSession) GetBranch() (string, error) {
	ctx := sql.NewContext(context.Background(), sql.WithSession(d))
	currentDb := d.Session.GetCurrentDatabase()

	// no branch if there's no current db
	if currentDb == "" {
		return "", nil
	}

	dbState, _, err := d.LookupDbState(ctx, currentDb)
	if err != nil {
		return "", err
	}

	if dbState.WorkingSet() != nil {
		branchRef, err := dbState.WorkingSet().Ref().ToHeadRef()
		if err != nil {
			return "", err
		}
		return branchRef.GetPath(), nil
	}
	// A nil working set probably means that we're not on a branch (like we may be on a commit), so we return an empty string
	return "", nil
}

// GetUser implements the interface branch_control.Context.
func (d *DoltSession) GetUser() string {
	return d.Session.Client().User
}

// GetHost implements the interface branch_control.Context.
func (d *DoltSession) GetHost() string {
	return d.Session.Client().Address
}

// GetController implements the interface branch_control.Context.
func (d *DoltSession) GetController() *branch_control.Controller {
	return d.branchController
}

// validatePersistedSysVar checks whether a system variable exists and is dynamic
func validatePersistableSysVar(name string) (sql.SystemVariable, interface{}, error) {
	sysVar, val, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return sql.SystemVariable{}, nil, sql.ErrUnknownSystemVariable.New(name)
	}
	if !sysVar.Dynamic {
		return sql.SystemVariable{}, nil, sql.ErrSystemVariableReadOnly.New(name)
	}
	return sysVar, val, nil
}

// getPersistedValue reads and converts a config value to the associated SystemVariable type
func getPersistedValue(conf config.ReadableConfig, k string) (interface{}, error) {
	v, err := conf.GetString(k)
	if err != nil {
		return nil, err
	}

	_, value, err := validatePersistableSysVar(k)
	if err != nil {
		return nil, err
	}

	var res interface{}
	switch value.(type) {
	case int8:
		var tmp int64
		tmp, err = strconv.ParseInt(v, 10, 8)
		res = int8(tmp)
	case int, int16, int32, int64:
		res, err = strconv.ParseInt(v, 10, 64)
	case uint, uint8, uint16, uint32, uint64:
		res, err = strconv.ParseUint(v, 10, 64)
	case float32, float64:
		res, err = strconv.ParseFloat(v, 64)
	case bool:
		return nil, sql.ErrInvalidType.New(value)
	case string:
		return v, nil
	default:
		return nil, sql.ErrInvalidType.New(value)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

// setPersistedValue casts and persists a key value pair assuming thread safety
func setPersistedValue(conf config.WritableConfig, key string, value interface{}) error {
	switch v := value.(type) {
	case int:
		return config.SetInt(conf, key, int64(v))
	case int8:
		return config.SetInt(conf, key, int64(v))
	case int16:
		return config.SetInt(conf, key, int64(v))
	case int32:
		return config.SetInt(conf, key, int64(v))
	case int64:
		return config.SetInt(conf, key, v)
	case uint:
		return config.SetUint(conf, key, uint64(v))
	case uint8:
		return config.SetUint(conf, key, uint64(v))
	case uint16:
		return config.SetUint(conf, key, uint64(v))
	case uint32:
		return config.SetUint(conf, key, uint64(v))
	case uint64:
		return config.SetUint(conf, key, v)
	case float32:
		return config.SetFloat(conf, key, float64(v))
	case float64:
		return config.SetFloat(conf, key, v)
	case string:
		return config.SetString(conf, key, v)
	case bool:
		return sql.ErrInvalidType.New(v)
	default:
		return sql.ErrInvalidType.New(v)
	}
}

// SystemVariablesInConfig returns system variables from the persisted config
// and a list of persisted keys that have no corresponding definition in
// |sql.SystemVariables|.
func SystemVariablesInConfig(conf config.ReadableConfig) ([]sql.SystemVariable, []string, error) {
	allVars := make([]sql.SystemVariable, conf.Size())
	var missingKeys []string
	i := 0
	var err error
	var sysVar sql.SystemVariable
	var def interface{}
	conf.Iter(func(k, v string) bool {
		def, err = getPersistedValue(conf, k)
		if err != nil {
			if sql.ErrUnknownSystemVariable.Is(err) {
				err = nil
				missingKeys = append(missingKeys, k)
				return false
			}
			err = fmt.Errorf("key: '%s'; %w", k, err)
			return true
		}
		// getPersistedVal already checked for errors
		sysVar, _, _ = sql.SystemVariables.GetGlobal(k)
		sysVar.Default = def
		allVars[i] = sysVar
		i++
		return false
	})
	if err != nil {
		return nil, nil, err
	}
	return allVars, missingKeys, nil
}

var initMu = sync.Mutex{}

func InitPersistedSystemVars(dEnv *env.DoltEnv) error {
	initMu.Lock()
	defer initMu.Unlock()

	var globals config.ReadWriteConfig
	if localConf, ok := dEnv.Config.GetConfig(env.LocalConfig); ok {
		globals = config.NewPrefixConfig(localConf, env.SqlServerGlobalsPrefix)
	} else if globalConf, ok := dEnv.Config.GetConfig(env.GlobalConfig); ok {
		globals = config.NewPrefixConfig(globalConf, env.SqlServerGlobalsPrefix)
	} else {
		cli.Println("warning: no local or global Dolt configuration found; session is not persistable")
		globals = config.NewMapConfig(make(map[string]string))
	}

	persistedGlobalVars, missingKeys, err := SystemVariablesInConfig(globals)
	if err != nil {
		return err
	}
	for _, k := range missingKeys {
		cli.Printf("warning: persisted system variable %s was not loaded since its definition does not exist.\n", k)
	}
	sql.SystemVariables.AddSystemVariables(persistedGlobalVars)
	return nil
}

// SplitRevisionDbName splits the given database name into its base and revision parts and returns them. Non-revision
// DBs use their full name as the base name, and empty string as the revision.
func SplitRevisionDbName(db SqlDatabase) (string, string) {
	sqldb, ok := db.(SqlDatabase)
	if !ok {
		return db.Name(), ""
	}

	dbName := db.Name()
	if sqldb.Revision() != "" {
		dbName = strings.TrimSuffix(dbName, DbRevisionDelimiter+sqldb.Revision())
	}

	return dbName, sqldb.Revision()
}

// TransactionRoot returns the noms root for the given database in the current transaction
func TransactionRoot(ctx *sql.Context, db SqlDatabase) (hash.Hash, error) {
	tx, ok := ctx.GetTransaction().(*DoltTransaction)
	// We don't have a real transaction in some cases (esp. PREPARE), in which case we need to use the tip of the data
	if !ok {
		return db.DbData().Ddb.NomsRoot(ctx)
	}

	baseName, _ := SplitRevisionDbName(db)
	nomsRoot, ok := tx.GetInitialRoot(baseName)
	if !ok {
		return hash.Hash{}, fmt.Errorf("could not resolve initial root for database %s", db.Name())
	}

	return nomsRoot, nil
}

const (
	DbRevisionDelimiter = "/"
)
