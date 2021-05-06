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

const EnableTransactionsEnvKey = "DOLT_ENABLE_TRANSACTIONS"

var transactionsEnabled = false

func init() {
	enableTx, ok := os.LookupEnv(EnableTransactionsEnvKey)
	if ok {
		if strings.ToLower(enableTx) == "true" {
			transactionsEnabled = true
		}
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
	roots        map[string]dbRoot
	workingSets  map[string]ref.WorkingSetRef
	dbDatas      map[string]env.DbData
	editSessions map[string]*editor.TableEditSession
	caches       map[string]TableCache
	Username     string
	Email        string
}

var _ sql.Session = &DoltSession{}

// DefaultDoltSession creates a DoltSession object with default values
func DefaultDoltSession() *DoltSession {
	sess := &DoltSession{
		Session:      sql.NewBaseSession(),
		roots:        make(map[string]dbRoot),
		dbDatas:      make(map[string]env.DbData),
		editSessions: make(map[string]*editor.TableEditSession),
		caches:       make(map[string]TableCache),
		workingSets:  make(map[string]ref.WorkingSetRef),
		Username:     "",
		Email:        "",
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
		Session:      sqlSess,
		dbDatas:      dbDatas,
		editSessions: editSessions,
		roots:        make(map[string]dbRoot),
		workingSets:  make(map[string]ref.WorkingSetRef),
		caches:       make(map[string]TableCache),
		Username:     username,
		Email:        email,
	}
	for _, db := range dbs {
		err := sess.AddDB(ctx, db)

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

func TableCacheFromSess(sess sql.Session, dbName string) TableCache {
	return sess.(*DoltSession).caches[dbName]
}

// CommitTransaction commits the in-progress transaction for the database named
func (sess *DoltSession) CommitTransaction(ctx *sql.Context, dbName string) error {
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
	if !transactionsEnabled {
		dbData := sess.dbDatas[dbName]

		h, err := dbData.Ddb.WriteRootValue(ctx, dbRoot.root)
		if err != nil {
			return err
		}

		return dbData.Rsw.SetWorkingHash(ctx, h)
	}

	// Newer commit path does a concurrent merge of the current root with the one other clients are editing, then
	// updates the session with this new root.
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}

	// TODO: validate that the transaction belongs to the DB named
	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	mergedRoot, err := dtx.Commit(ctx, dbRoot.root)
	if err != nil {
		return err
	}

	return sess.SetRoot(ctx, dbName, mergedRoot)
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

// SetRoot sets a new root value for the session for the database named.
// Can be used if the dolt working set value changes outside of the basic SQL execution engine.
// If |newRoot|'s FeatureVersion is out-of-date with the client, SetRoot will update it.
func (sess *DoltSession) SetRoot(ctx *sql.Context, dbName string, newRoot *doltdb.RootValue) error {
	h, err := newRoot.HashOf()
	if err != nil {
		return err
	}

	hashStr := h.String()
	key := WorkingKey(dbName)

	err = ctx.Session.SetSessionVariable(ctx, key, hashStr)

	if err != nil {
		return err
	}

	sess.roots[dbName] = dbRoot{hashStr, newRoot}

	err = sess.editSessions[dbName].SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}

	return nil
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
		return sess.setWorkingSessionVar(ctx, key, value, dbName)
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

func (sess *DoltSession) setWorkingSessionVar(ctx *sql.Context, key string, value interface{}, dbName string) error {
	valStr, isStr := value.(string) // valStr represents a root val hash
	if !isStr || !hash.IsValid(valStr) {
		return doltdb.ErrInvalidHash
	}

	err := sess.Session.SetSessionVariable(ctx, key, valStr)
	if err != nil {
		return err
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

	sess.roots[dbName] = dbRoot{valStr, root}

	err = sess.editSessions[dbName].SetRoot(ctx, root)
	if err != nil {
		return err
	}

	sess.caches[dbName].Clear()

	return nil
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

	h, err := root.HashOf()
	if err != nil {
		return err
	}

	err = sess.Session.SetSessionVariable(ctx, key, value)
	if err != nil {
		return err
	}

	hashStr := h.String()
	// TODO: this needs to use the shared working set, if present
	err = sess.Session.SetSessionVariable(ctx, WorkingKey(dbName), hashStr)
	if err != nil {
		return err
	}

	sess.roots[dbName] = dbRoot{hashStr, root}

	err = sess.editSessions[dbName].SetRoot(ctx, root)
	if err != nil {
		return err
	}

	sess.caches[dbName].Clear()

	return nil
}

// SetSessionVarDirectly directly updates sess.Session. This is useful in the context of the sql shell where
// the working and head session variable may be updated at different times.
func (sess *DoltSession) SetSessionVarDirectly(ctx *sql.Context, key string, value interface{}) error {
	return sess.Session.SetSessionVariable(ctx, key, value)
}

// AddDB adds the database given to this session. This establishes a starting root value for this session, as well as
// other state tracking metadata.
func (sess *DoltSession) AddDB(ctx *sql.Context, db Database) error {
	defineSystemVariables(db.Name())

	rsr := db.GetStateReader()
	rsw := db.GetStateWriter()
	drw := db.GetDocsReadWriter()
	ddb := db.GetDoltDB()

	sess.dbDatas[db.Name()] = env.DbData{Drw: drw, Rsr: rsr, Rsw: rsw, Ddb: ddb}
	sess.editSessions[db.Name()] = editor.CreateTableEditSession(nil, editor.TableEditSessionProps{})
	sess.caches[db.name] = newTableCache()

	cs := rsr.CWBHeadSpec()
	headRef := rsr.CWBHeadRef()

	// Not all dolt commands update the working set ref yet. So until that's true, we update it here with the contents
	// of the repo_state.json file
	workingSetRef, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return err
	}
	sess.workingSets[db.name] = workingSetRef

	var workingRoot *doltdb.RootValue
	workingHashInRepoState := rsr.WorkingHash()
	workingHashInWsRef := hash.Hash{}

	workingRoot, err = ddb.ReadRootValue(ctx, workingHashInRepoState)
	if err != nil {
		return err
	}

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

	err = ddb.UpdateWorkingSet(ctx, workingSetRef, workingRoot, workingHashInWsRef)
	if err != nil {
		return err
	}

	sess.roots[db.name] = dbRoot{
		hashStr: workingHashInRepoState.String(),
		root:    workingRoot,
	}

	cm, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}

	headCommitHash, err := cm.HashOf()
	if err != nil {
		return err
	}

	return sess.setSessionVars(ctx, db, workingSetRef, headCommitHash, workingHashInRepoState)
}

// setSessionVars sets the dolt-specific session vars for the database given to the values given.
func (sess *DoltSession) setSessionVars(
	ctx *sql.Context,
	db Database,
	workingSetRef ref.WorkingSetRef,
	headCommitHash hash.Hash,
	workingRootHash hash.Hash,
) error {
	err := sess.Session.SetSessionVariable(ctx, HeadRefKey(db.Name()), workingSetRef.GetPath())
	if err != nil {
		return err
	}

	err = sess.Session.SetSessionVariable(ctx, HeadKey(db.Name()), headCommitHash.String())
	if err != nil {
		return err
	}

	err = sess.Session.SetSessionVariable(ctx, WorkingKey(db.Name()), workingRootHash.String())
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
		})
	}
}
