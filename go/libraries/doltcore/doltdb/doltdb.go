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

package doltdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

func init() {
	types.CreateEditAccForMapEdits = edits.NewAsyncSortedEditsWithDefaults
}

// WORKING and STAGED identifiers refer to the working and staged roots in special circumstances where
// we expect to resolve a commit spec, but need working or staged
const (
	Working = "WORKING"
	Staged  = "STAGED"
)

const (
	CreationBranch = "create"

	defaultChunksPerTF = 256 * 1024
)

var ErrMissingDoltDataDir = errors.New("missing dolt data directory")

// LocalDirDoltDB stores the db in the current directory
var LocalDirDoltDB = "file://./" + dbfactory.DoltDataDir
var LocalDirStatsDB = "file://./" + dbfactory.DoltStatsDir

// InMemDoltDB stores the DoltDB db in memory and is primarily used for testing
var InMemDoltDB = "mem://"

var ErrNoRootValAtHash = errors.New("there is no dolt root value at that hash")
var ErrCannotDeleteLastBranch = errors.New("cannot delete the last branch")

// DoltDB wraps access to the underlying noms database and hides some of the details of the underlying storage.
type DoltDB struct {
	db  hooksDatabase
	vrw types.ValueReadWriter
	ns  tree.NodeStore

	// databaseName holds the name of the database for this DoltDB instance. Note that this name may not be
	// populated for all DoltDB instances. For filesystem based databases, the database name is determined
	// by looking through the filepath in reverse, finding the first .dolt directory, and then taking the
	// parent directory as the database name. For non-filesystem based databases, the database name will not
	// currently be populated.
	databaseName string
}

// DoltDBFromCS creates a DoltDB from a noms chunks.ChunkStore
func DoltDBFromCS(cs chunks.ChunkStore) *DoltDB {
	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db := datas.NewTypesDatabase(vrw, ns)

	return &DoltDB{db: hooksDatabase{Database: db}, vrw: vrw, ns: ns}
}

// HackDatasDatabaseFromDoltDB unwraps a DoltDB to a datas.Database.
// Deprecated: only for use in dolt migrate.
func HackDatasDatabaseFromDoltDB(ddb *DoltDB) datas.Database {
	return ddb.db
}

// LoadDoltDB will acquire a reference to the underlying noms db.  If the Location is InMemDoltDB then a reference
// to a newly created in memory database will be used. If the location is LocalDirDoltDB, the directory must exist or
// this returns nil.
func LoadDoltDB(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, fs filesys.Filesys) (*DoltDB, error) {
	return LoadDoltDBWithParams(ctx, nbf, urlStr, fs, nil)
}

func LoadDoltDBWithParams(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, fs filesys.Filesys, params map[string]interface{}) (*DoltDB, error) {
	if urlStr == LocalDirDoltDB {
		exists, isDir := fs.Exists(dbfactory.DoltDataDir)
		if !exists {
			return nil, ErrMissingDoltDataDir
		} else if !isDir {
			return nil, errors.New("file exists where the dolt data directory should be")
		}

		absPath, err := fs.Abs(dbfactory.DoltDataDir)
		if err != nil {
			return nil, err
		}

		urlStr = earl.FileUrlFromPath(filepath.ToSlash(absPath), os.PathSeparator)

		if params == nil {
			params = make(map[string]any)
		}
		params[dbfactory.ChunkJournalParam] = struct{}{}
	}

	// Pull the database name out of the URL string. For filesystem-based databases (e.g. in-memory or disk-based
	// filesystem implementations), we can determine the database name by looking at the filesystem path. This
	// won't work for other storage schemes though.
	name := findParentDirectory(urlStr, ".dolt")

	db, vrw, ns, err := dbfactory.CreateDB(ctx, nbf, urlStr, params)
	if err != nil {
		return nil, err
	}
	return &DoltDB{db: hooksDatabase{Database: db}, vrw: vrw, ns: ns, databaseName: name}, nil
}

// NomsRoot returns the hash of the noms dataset map
func (ddb *DoltDB) NomsRoot(ctx context.Context) (hash.Hash, error) {
	return datas.ChunkStoreFromDatabase(ddb.db).Root(ctx)
}

func (ddb *DoltDB) AccessMode() chunks.ExclusiveAccessMode {
	return datas.ChunkStoreFromDatabase(ddb.db).AccessMode()
}

// CommitRoot executes a chunkStore commit, atomically swapping the root hash of the database manifest
func (ddb *DoltDB) CommitRoot(ctx context.Context, current, last hash.Hash) (bool, error) {
	return datas.ChunkStoreFromDatabase(ddb.db).Commit(ctx, current, last)
}

func (ddb *DoltDB) Has(ctx context.Context, h hash.Hash) (bool, error) {
	return datas.ChunkStoreFromDatabase(ddb.db).Has(ctx, h)
}

func (ddb *DoltDB) CSMetricsSummary() string {
	return datas.GetCSStatSummaryForDB(ddb.db)
}

// WriteEmptyRepo will create initialize the given db with a master branch which points to a commit which has valid
// metadata for the creation commit, and an empty RootValue.
func (ddb *DoltDB) WriteEmptyRepo(ctx context.Context, initBranch, name, email string) error {
	return ddb.WriteEmptyRepoWithCommitMetaGenerator(ctx, initBranch, datas.MakeCommitMetaGenerator(name, email, datas.CommitterDate()))
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitMetaGenerator(ctx context.Context, initBranch string, commitMeta datas.CommitMetaGenerator) error {
	return ddb.WriteEmptyRepoWithCommitMetaGeneratorAndDefaultBranch(ctx, commitMeta, ref.NewBranchRef(initBranch))
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitTimeAndDefaultBranch(
	ctx context.Context,
	name, email string,
	t time.Time,
	init ref.BranchRef,
) error {
	return ddb.WriteEmptyRepoWithCommitMetaGeneratorAndDefaultBranch(ctx, datas.MakeCommitMetaGenerator(name, email, t), init)
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitMetaGeneratorAndDefaultBranch(
	ctx context.Context,
	commitMetaGenerator datas.CommitMetaGenerator,
	init ref.BranchRef,
) error {
	ds, err := ddb.db.GetDataset(ctx, CreationBranch)

	if err != nil {
		return err
	}

	if ds.HasHead() {
		return errors.New("database already exists")
	}

	rv, err := EmptyRootValue(ctx, ddb.vrw, ddb.ns)

	if err != nil {
		return err
	}

	rv, _, err = ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	var firstCommit *datas.Commit
	for {
		cm, err := commitMetaGenerator.Next()
		if err != nil {
			return err
		}

		commitOpts := datas.CommitOptions{Meta: cm}

		cb := ref.NewInternalRef(CreationBranch)
		ds, err = ddb.db.GetDataset(ctx, cb.String())
		if err != nil {
			return err
		}

		firstCommit, err = ddb.db.BuildNewCommit(ctx, ds, rv.NomsValue(), commitOpts)
		if err != nil {
			return err
		}

		if commitMetaGenerator.IsGoodCommit(firstCommit) {
			break
		}
	}

	firstCommitDs, err := ddb.db.WriteCommit(ctx, ds, firstCommit)

	if err != nil {
		return err
	}

	ds, err = ddb.db.GetDataset(ctx, init.String())

	if err != nil {
		return err
	}

	headAddr, ok := firstCommitDs.MaybeHeadAddr()
	if !ok {
		return errors.New("commit without head")
	}

	_, err = ddb.db.SetHead(ctx, ds, headAddr, "")
	return err
}

func (ddb *DoltDB) Close() error {
	return ddb.db.Close()
}

// GetHashForRefStr resolves a ref string (such as a branch name or tag) and resolves it to a hash.Hash.
func (ddb *DoltDB) GetHashForRefStr(ctx context.Context, ref string) (*hash.Hash, error) {
	if err := datas.ValidateDatasetId(ref); err != nil {
		return nil, fmt.Errorf("invalid ref format: %s", ref)
	}

	ds, err := ddb.db.GetDataset(ctx, ref)

	if err != nil {
		return nil, err
	}

	return hashOfCommit(ds, ref)
}

func (ddb *DoltDB) GetHashForRefStrByNomsRoot(ctx context.Context, ref string, nomsRoot hash.Hash) (*hash.Hash, error) {
	if err := datas.ValidateDatasetId(ref); err != nil {
		return nil, fmt.Errorf("invalid ref format: %s", ref)
	}

	ds, err := ddb.db.GetDatasetByRootHash(ctx, ref, nomsRoot)
	if err != nil {
		return nil, err
	}

	return hashOfCommit(ds, ref)
}

// hashOfCommit returns the hash of the commit at the head of the dataset provided
func hashOfCommit(ds datas.Dataset, ref string) (*hash.Hash, error) {
	if !ds.HasHead() {
		return nil, ErrBranchNotFound
	}

	if ds.IsTag() {
		_, commitHash, err := ds.HeadTag()
		if err != nil {
			return nil, err
		}
		return &commitHash, nil
	} else {
		commitHash, ok := ds.MaybeHeadAddr()
		if !ok {
			return nil, fmt.Errorf("Unable to load head for %s", ref)
		}
		return &commitHash, nil
	}
}

func getCommitValForRefStr(ctx context.Context, ddb *DoltDB, ref string) (*datas.Commit, error) {
	commitHash, err := ddb.GetHashForRefStr(ctx, ref)

	if err != nil {
		return nil, err
	}

	return datas.LoadCommitAddr(ctx, ddb.vrw, *commitHash)
}

func getCommitValForRefStrByNomsRoot(ctx context.Context, ddb *DoltDB, ref string, nomsRoot hash.Hash) (*datas.Commit, error) {
	commitHash, err := ddb.GetHashForRefStrByNomsRoot(ctx, ref, nomsRoot)

	if err != nil {
		return nil, err
	}

	return datas.LoadCommitAddr(ctx, ddb.vrw, *commitHash)
}

// findParentDirectory searches the components of the specified |path| looking for a directory
// named |targetDir| and returns the name of the parent directory for |targetDir|. The search
// starts from the deepest component of |path|, so if |path| contains |targetDir| multiple times,
// the parent directory of the last occurrence in |path| is returned.
func findParentDirectory(path string, targetDir string) string {
	base := filepath.Base(path)
	dir := filepath.Dir(path)

	if base == "." || dir == "." {
		return ""
	}

	switch base {
	case "":
		return base

	case targetDir:
		return filepath.Base(dir)

	default:
		return findParentDirectory(dir, targetDir)
	}
}

// Roots is a convenience struct to package up the three roots that most library functions will need to inspect and
// modify the working set. This struct is designed to be passed by value always: functions should take a Roots as a
// param and return a modified one.
//
// It contains three root values:
// Head: The root of the head of the current working branch
// Working: The root of the current working set
// Staged: The root of the staged value
//
// See doltEnvironment.Roots(context.Context)
type Roots struct {
	Head    RootValue
	Working RootValue
	Staged  RootValue
}

func (ddb *DoltDB) getHashFromCommitSpec(ctx context.Context, cs *CommitSpec, cwb ref.DoltRef, nomsRoot hash.Hash) (*hash.Hash, error) {
	switch cs.csType {
	case hashCommitSpec:
		parsedHash, ok := hash.MaybeParse(cs.baseSpec)
		if !ok {
			return nil, errors.New("invalid hash: " + cs.baseSpec)
		}
		return &parsedHash, nil
	case refCommitSpec:
		// For a ref in a CommitSpec, we have the following behavior.
		// If it starts with `refs/`, we look for an exact match before
		// we try any suffix matches. After that, we try a match on the
		// user supplied input, with the following four prefixes, in
		// order: `refs/`, `refs/heads/`, `refs/tags/`, `refs/remotes/`.
		candidates := []string{
			"refs/" + cs.baseSpec,
			"refs/heads/" + cs.baseSpec,
			"refs/tags/" + cs.baseSpec,
			"refs/remotes/" + cs.baseSpec,
		}
		if strings.HasPrefix(cs.baseSpec, "refs/") {
			candidates = []string{
				cs.baseSpec,
				"refs/" + cs.baseSpec,
				"refs/heads/" + cs.baseSpec,
				"refs/tags/" + cs.baseSpec,
				"refs/remotes/" + cs.baseSpec,
			}
		}
		for _, candidate := range candidates {
			var valueHash *hash.Hash
			var err error
			if nomsRoot.IsEmpty() {
				valueHash, err = ddb.GetHashForRefStr(ctx, candidate)
			} else {
				valueHash, err = ddb.GetHashForRefStrByNomsRoot(ctx, candidate, nomsRoot)
			}
			if err == nil {
				return valueHash, nil
			}
			if err != ErrBranchNotFound {
				return nil, err
			}
		}
		return nil, fmt.Errorf("%w: %s", ErrBranchNotFound, cs.baseSpec)
	case headCommitSpec:
		if cwb == nil {
			return nil, fmt.Errorf("cannot use a nil current working branch with a HEAD commit spec")
		}
		if nomsRoot.IsEmpty() {
			return ddb.GetHashForRefStr(ctx, cwb.String())
		} else {
			return ddb.GetHashForRefStrByNomsRoot(ctx, cwb.String(), nomsRoot)
		}
	default:
		panic("unrecognized commit spec csType: " + cs.csType)
	}
}

// Resolve takes a CommitSpec and returns a Commit, or an error if the commit cannot be found.
// If the CommitSpec is HEAD, Resolve also needs the DoltRef of the current working branch.
func (ddb *DoltDB) Resolve(ctx context.Context, cs *CommitSpec, cwb ref.DoltRef) (*OptionalCommit, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	hash, err := ddb.getHashFromCommitSpec(ctx, cs, cwb, hash.Hash{})
	if err != nil {
		return nil, err
	}

	commitValue, err := datas.LoadCommitAddr(ctx, ddb.vrw, *hash)
	if err != nil {
		return nil, err
	}

	if commitValue.IsGhost() {
		return &OptionalCommit{nil, *hash}, nil
	}

	commit, err := NewCommit(ctx, ddb.vrw, ddb.ns, commitValue)
	if err != nil {
		return nil, err
	}

	return commit.GetAncestor(ctx, cs.aSpec)
}

// BootstrapShallowResolve is a special case of Resolve that is used to resolve a commit prior to pulling it's history
// in a shallow clone. In general, application code should call Resolve and get an OptionalCommit. This is a special case
// where we need to get the head commit for the commit closure used to determine what commits should skipped.
func (ddb *DoltDB) BootstrapShallowResolve(ctx context.Context, cs *CommitSpec) (prolly.CommitClosure, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	hash, err := ddb.getHashFromCommitSpec(ctx, cs, nil, hash.Hash{})
	if err != nil {
		return prolly.CommitClosure{}, err
	}

	commitValue, err := datas.LoadCommitAddr(ctx, ddb.vrw, *hash)
	if err != nil {
		return prolly.CommitClosure{}, err
	}

	if commitValue.IsGhost() {
		return prolly.CommitClosure{}, ErrGhostCommitEncountered
	}

	return getCommitClosure(ctx, commitValue, ddb.vrw, ddb.ns)
}

func (ddb *DoltDB) ResolveByNomsRoot(ctx *sql.Context, cs *CommitSpec, cwb ref.DoltRef, root hash.Hash) (*OptionalCommit, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	hash, err := ddb.getHashFromCommitSpec(ctx, cs, cwb, root)
	if err != nil {
		return nil, err
	}

	commitValue, err := datas.LoadCommitAddr(ctx, ddb.vrw, *hash)
	if err != nil {
		return nil, err
	}

	if commitValue.IsGhost() {
		return &OptionalCommit{nil, *hash}, nil
	}

	commit, err := NewCommit(ctx, ddb.vrw, ddb.ns, commitValue)
	if err != nil {
		return nil, err
	}
	return commit.GetAncestor(ctx, cs.aSpec)
}

// ResolveCommitRef takes a DoltRef and returns a Commit, or an error if the commit cannot be found. The ref given must
// point to a Commit.
func (ddb *DoltDB) ResolveCommitRef(ctx context.Context, ref ref.DoltRef) (*Commit, error) {
	commitVal, err := getCommitValForRefStr(ctx, ddb, ref.String())
	if err != nil {
		return nil, err
	}

	if commitVal.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	return NewCommit(ctx, ddb.vrw, ddb.ns, commitVal)
}

// ResolveCommitRefAtRoot takes a DoltRef and returns a Commit, or an error if the commit cannot be found. The ref given must
// point to a Commit.
func (ddb *DoltDB) ResolveCommitRefAtRoot(ctx context.Context, ref ref.DoltRef, nomsRoot hash.Hash) (*Commit, error) {
	commitVal, err := getCommitValForRefStrByNomsRoot(ctx, ddb, ref.String(), nomsRoot)
	if err != nil {
		return nil, err
	}

	if commitVal.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	return NewCommit(ctx, ddb.vrw, ddb.ns, commitVal)
}

// ResolveBranchRoots returns the Roots for the branch given
func (ddb *DoltDB) ResolveBranchRoots(ctx context.Context, branch ref.BranchRef) (Roots, error) {
	commitRef, err := ddb.ResolveCommitRef(ctx, branch)
	if err != nil {
		return Roots{}, err
	}

	headRoot, err := commitRef.GetRootValue(ctx)
	if err != nil {
		return Roots{}, err
	}

	wsRef, err := ref.WorkingSetRefForHead(branch)
	if err != nil {
		return Roots{}, err
	}

	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return Roots{}, err
	}

	return Roots{
		Head:    headRoot,
		Working: ws.WorkingRoot(),
		Staged:  ws.StagedRoot(),
	}, nil
}

// ResolveTag takes a TagRef and returns the corresponding Tag object.
func (ddb *DoltDB) ResolveTag(ctx context.Context, tagRef ref.TagRef) (*Tag, error) {
	ds, err := ddb.db.GetDataset(ctx, tagRef.String())
	if err != nil {
		return nil, ErrTagNotFound
	}

	if !ds.HasHead() {
		return nil, ErrTagNotFound
	}

	if !ds.IsTag() {
		return nil, fmt.Errorf("tagRef head is not a tag")
	}

	return NewTag(ctx, tagRef.GetPath(), ds, ddb.vrw, ddb.ns)
}

// ResolveWorkingSet takes a WorkingSetRef and returns the corresponding WorkingSet object.
func (ddb *DoltDB) ResolveWorkingSet(ctx context.Context, workingSetRef ref.WorkingSetRef) (*WorkingSet, error) {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())

	if err != nil {
		return nil, ErrWorkingSetNotFound
	}

	return ddb.workingSetFromDataset(ctx, workingSetRef, ds)
}

// ResolveWorkingSetAtRoot returns the working set object as it existed at the given root hash.
func (ddb *DoltDB) ResolveWorkingSetAtRoot(ctx context.Context, workingSetRef ref.WorkingSetRef, nomsRoot hash.Hash) (*WorkingSet, error) {
	ds, err := ddb.db.GetDatasetByRootHash(ctx, workingSetRef.String(), nomsRoot)

	if err != nil {
		return nil, ErrWorkingSetNotFound
	}

	return ddb.workingSetFromDataset(ctx, workingSetRef, ds)
}

func (ddb *DoltDB) workingSetFromDataset(ctx context.Context, workingSetRef ref.WorkingSetRef, ds datas.Dataset) (*WorkingSet, error) {
	if !ds.HasHead() {
		return nil, ErrWorkingSetNotFound
	}

	if !ds.IsWorkingSet() {
		return nil, fmt.Errorf("workingSetRef head is not a workingSetRef")
	}

	return newWorkingSet(ctx, workingSetRef.GetPath(), ddb.vrw, ddb.ns, ds)
}

// TODO: convenience method to resolve the head commit of a branch.

// WriteRootValue will write a doltdb.RootValue instance to the database.  This
// value will not be associated with a commit and can be committed by hash at a
// later time.  Returns an updated root value and the hash of the value
// written.  This method is the primary place in doltcore that handles setting
// the FeatureVersion of root values to the current value, so all writes of
// RootValues should happen here.
func (ddb *DoltDB) WriteRootValue(ctx context.Context, rv RootValue) (RootValue, hash.Hash, error) {
	nrv, ref, err := ddb.writeRootValue(ctx, rv)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	return nrv, ref.TargetHash(), nil
}

func (ddb *DoltDB) writeRootValue(ctx context.Context, rv RootValue) (RootValue, types.Ref, error) {
	rv, err := rv.SetFeatureVersion(DoltFeatureVersion)
	if err != nil {
		return nil, types.Ref{}, err
	}
	ref, err := ddb.vrw.WriteValue(ctx, rv.NomsValue())
	if err != nil {
		return nil, types.Ref{}, err
	}
	return rv, ref, nil
}

// ReadRootValue reads the RootValue associated with the hash given and returns it. Returns an error if the value cannot
// be read, or if the hash given doesn't represent a dolt RootValue.
func (ddb *DoltDB) ReadRootValue(ctx context.Context, h hash.Hash) (RootValue, error) {
	val, err := ddb.vrw.ReadValue(ctx, h)
	if err != nil {
		return nil, err
	}
	return decodeRootNomsValue(ctx, ddb.vrw, ddb.ns, val)
}

// ReadCommit reads the Commit whose hash is |h|, if one exists.
func (ddb *DoltDB) ReadCommit(ctx context.Context, h hash.Hash) (*OptionalCommit, error) {
	c, err := datas.LoadCommitAddr(ctx, ddb.vrw, h)
	if err != nil {
		return nil, err
	}

	if c.IsGhost() {
		return &OptionalCommit{nil, h}, nil
	}

	newC, err := NewCommit(ctx, ddb.vrw, ddb.ns, c)
	if err != nil {
		return nil, err
	}
	return &OptionalCommit{newC, h}, nil
}

// Commit will update a branch's head value to be that of a previously committed root value hash
func (ddb *DoltDB) Commit(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, cm *datas.CommitMeta) (*Commit, error) {
	return ddb.CommitWithParentSpecs(ctx, valHash, dref, nil, cm)
}

// FastForwardWithWorkspaceCheck will perform a fast forward update of the branch given to the commit given, but only
// if the working set is in sync with the head of the branch given. This is used in the course of pushing to a remote.
// If the target doesn't currently have the working set ref, then no working set change will be made.
func (ddb *DoltDB) FastForwardWithWorkspaceCheck(ctx context.Context, branch ref.DoltRef, commit *Commit) error {
	ds, err := ddb.db.GetDataset(ctx, branch.String())
	if err != nil {
		return err
	}

	addr, err := commit.HashOf()
	if err != nil {
		return err
	}

	ws := ""
	pushConcurrencyControl := chunks.GetPushConcurrencyControl(datas.ChunkStoreFromDatabase(ddb.db))
	if pushConcurrencyControl == chunks.PushConcurrencyControl_AssertWorkingSet {
		wsRef, err := ref.WorkingSetRefForHead(branch)
		if err != nil {
			return err
		}
		ws = wsRef.String()
	}

	_, err = ddb.db.FastForward(ctx, ds, addr, ws)

	return err
}

// FastForward fast-forwards the branch given to the commit given.
func (ddb *DoltDB) FastForward(ctx context.Context, branch ref.DoltRef, commit *Commit) error {
	addr, err := commit.HashOf()
	if err != nil {
		return err
	}

	return ddb.FastForwardToHash(ctx, branch, addr)
}

// FastForwardToHash fast-forwards the branch given to the commit hash given.
func (ddb *DoltDB) FastForwardToHash(ctx context.Context, branch ref.DoltRef, hash hash.Hash) error {
	ds, err := ddb.db.GetDataset(ctx, branch.String())
	if err != nil {
		return err
	}

	_, err = ddb.db.FastForward(ctx, ds, hash, "")

	return err
}

// CanFastForward returns whether the given branch can be fast-forwarded to the commit given.
func (ddb *DoltDB) CanFastForward(ctx context.Context, branch ref.DoltRef, new *Commit) (bool, error) {
	current, err := ddb.ResolveCommitRef(ctx, branch)

	if err != nil {
		if err == ErrBranchNotFound {
			return true, nil
		}

		return false, err
	}

	return current.CanFastForwardTo(ctx, new)
}

// SetHeadToCommit sets the given ref to point at the given commit. It is used in the course of 'force' updates.
func (ddb *DoltDB) SetHeadToCommit(ctx context.Context, ref ref.DoltRef, cm *Commit) error {
	addr, err := cm.HashOf()
	if err != nil {
		return err
	}

	return ddb.SetHead(ctx, ref, addr)
}

// SetHeadAndWorkingSetToCommit sets the given ref to the given commit, and ensures that working is in sync
// with the head. Used for 'force' pushes.
func (ddb *DoltDB) SetHeadAndWorkingSetToCommit(ctx context.Context, rf ref.DoltRef, cm *Commit) error {
	addr, err := cm.HashOf()
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(rf)
	if err != nil {
		return err
	}

	ds, err := ddb.db.GetDataset(ctx, rf.String())
	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, addr, wsRef.String())
	return err
}

func (ddb *DoltDB) SetHead(ctx context.Context, ref ref.DoltRef, addr hash.Hash) error {
	ds, err := ddb.db.GetDataset(ctx, ref.String())

	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, addr, "")
	return err
}

// CommitWithParentSpecs commits the value hash given to the branch given, using the list of parent hashes given. Returns an
// error if the value or any parents can't be resolved, or if anything goes wrong accessing the underlying storage.
func (ddb *DoltDB) CommitWithParentSpecs(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, parentCmSpecs []*CommitSpec, cm *datas.CommitMeta) (*Commit, error) {
	var parentCommits []*Commit
	for _, parentCmSpec := range parentCmSpecs {
		cm, err := ddb.Resolve(ctx, parentCmSpec, nil)
		if err != nil {
			return nil, err
		}

		hardCommit, ok := cm.ToCommit()
		if !ok {
			return nil, ErrGhostCommitEncountered
		}

		parentCommits = append(parentCommits, hardCommit)
	}
	return ddb.CommitWithParentCommits(ctx, valHash, dref, parentCommits, cm)
}

func (ddb *DoltDB) CommitWithParentCommits(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, parentCommits []*Commit, cm *datas.CommitMeta) (*Commit, error) {
	val, err := ddb.vrw.ReadValue(ctx, valHash)

	if err != nil {
		return nil, err
	}

	if !isRootValue(ddb.vrw.Format(), val) {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	ds, err := ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return nil, err
	}

	var parents []hash.Hash
	headAddr, hasHead := ds.MaybeHeadAddr()
	if err != nil {
		return nil, err
	}
	if hasHead {
		parents = append(parents, headAddr)
	}

	for _, cm := range parentCommits {
		addr, err := cm.HashOf()
		if err != nil {
			return nil, err
		}
		if addr != headAddr {
			parents = append(parents, addr)
		}
	}
	commitOpts := datas.CommitOptions{Parents: parents, Meta: cm}

	return ddb.CommitValue(ctx, dref, val, commitOpts)
}

func (ddb *DoltDB) CommitValue(ctx context.Context, dref ref.DoltRef, val types.Value, commitOpts datas.CommitOptions) (*Commit, error) {
	ds, err := ddb.db.GetDataset(ctx, dref.String())
	if err != nil {
		return nil, err
	}

	ds, err = ddb.db.Commit(ctx, ds, val, commitOpts)
	if err != nil {
		return nil, err
	}

	r, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Commit has no head but commit succeeded. This is a bug.")
	}

	dc, err := datas.LoadCommitRef(ctx, ddb.vrw, r)
	if err != nil {
		return nil, err
	}

	if dc.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	return NewCommit(ctx, ddb.vrw, ddb.ns, dc)
}

// dangling commits are unreferenced by any branch or ref. They are created in the course of programmatic updates
// such as rebase. You must create a ref to a dangling commit for it to be reachable
func (ddb *DoltDB) CommitDanglingWithParentCommits(ctx context.Context, valHash hash.Hash, parentCommits []*Commit, cm *datas.CommitMeta) (*Commit, error) {
	val, err := ddb.vrw.ReadValue(ctx, valHash)
	if err != nil {
		return nil, err
	}
	if !isRootValue(ddb.vrw.Format(), val) {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	var parents []hash.Hash
	for _, cm := range parentCommits {
		addr, err := cm.HashOf()
		if err != nil {
			return nil, err
		}
		parents = append(parents, addr)
	}
	commitOpts := datas.CommitOptions{Parents: parents, Meta: cm}

	return ddb.CommitDangling(ctx, val, commitOpts)
}

// CommitDangling creates a new Commit for |val| that is not referenced by any DoltRef.
func (ddb *DoltDB) CommitDangling(ctx context.Context, val types.Value, opts datas.CommitOptions) (*Commit, error) {
	cs := datas.ChunkStoreFromDatabase(ddb.db)

	dcommit, err := datas.NewCommitForValue(ctx, cs, ddb.vrw, ddb.ns, val, opts)
	if err != nil {
		return nil, err
	}

	_, err = ddb.vrw.WriteValue(ctx, dcommit.NomsValue())
	if err != nil {
		return nil, err
	}

	return NewCommit(ctx, ddb.vrw, ddb.ns, dcommit)
}

// ValueReadWriter returns the underlying noms database as a types.ValueReadWriter.
func (ddb *DoltDB) ValueReadWriter() types.ValueReadWriter {
	return ddb.vrw
}

func (ddb *DoltDB) NodeStore() tree.NodeStore {
	return ddb.ns
}

func (ddb *DoltDB) Format() *types.NomsBinFormat {
	return ddb.vrw.Format()
}

// ResolveParent returns the n-th ancestor of a given commit (direct parent is index 0). error return value will be
// non-nil in the case that the commit cannot be resolved, there aren't as many ancestors as requested, or the
// underlying storage cannot be accessed.
func (ddb *DoltDB) ResolveParent(ctx context.Context, commit *Commit, parentIdx int) (*OptionalCommit, error) {
	return commit.GetParent(ctx, parentIdx)
}

func (ddb *DoltDB) ResolveAllParents(ctx context.Context, commit *Commit) ([]*OptionalCommit, error) {
	num := commit.NumParents()
	resolved := make([]*OptionalCommit, num)
	for i := 0; i < num; i++ {
		parent, err := ddb.ResolveParent(ctx, commit, i)
		if err != nil {
			return nil, err
		}
		resolved[i] = parent
	}
	return resolved, nil
}

// HasRef returns whether the branch given exists in this database.
func (ddb *DoltDB) HasRef(ctx context.Context, doltRef ref.DoltRef) (bool, error) {
	ds, err := ddb.db.GetDataset(ctx, doltRef.String())
	if err != nil {
		if errors.Is(err, datas.ErrInvalidDatasetID) {
			return false, nil
		}
		return false, err
	}
	return ds.HasHead(), nil
}

var branchRefFilter = map[ref.RefType]struct{}{ref.BranchRefType: {}}

// GetBranches returns a list of all branches in the database.
func (ddb *DoltDB) GetBranches(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, branchRefFilter)
}

// GetBranches returns a list of all branches in the database.
func (ddb *DoltDB) GetBranchesByNomsRoot(ctx context.Context, nomsRoot hash.Hash) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfTypeByNomsRoot(ctx, branchRefFilter, nomsRoot)
}

// HasBranch returns whether the DB has a branch with the name given, case-insensitive. Returns the case-sensitive
// matching branch if found, as well as a bool indicating if there was a case-insensitive match, and any error.
func (ddb *DoltDB) HasBranch(ctx context.Context, branchName string) (string, bool, error) {
	branches, err := ddb.GetRefsOfType(ctx, branchRefFilter)
	if err != nil {
		return "", false, err
	}

	for _, b := range branches {
		if path := b.GetPath(); strings.EqualFold(path, branchName) {
			return path, true, nil
		}
	}

	return "", false, nil
}

// HasRemoteTrackingBranch returns whether the DB has a remote tracking branch with the name given, case-insensitive.
// Returns the case-sensitive matching branch if found, as well as a bool indicating if there was a case-insensitive match,
// remote tracking branchRef that is the only match for the branchName and any error.
func (ddb *DoltDB) HasRemoteTrackingBranch(ctx context.Context, branchName string) (string, bool, ref.RemoteRef, error) {
	remoteRefFound := false
	var remoteRef ref.RemoteRef

	remoteRefs, err := ddb.GetRemoteRefs(ctx)
	if err != nil {
		return "", false, ref.RemoteRef{}, err
	}

	for _, rf := range remoteRefs {
		if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == branchName {
			if remoteRefFound {
				// if there are multiple remotes with matching branch names with defined branch name, it errors
				return "", false, ref.RemoteRef{}, fmt.Errorf("'%s' matched multiple remote tracking branches", branchName)
			}
			remoteRefFound = true
			remoteRef = remRef
		}
	}

	if remoteRefFound {
		return branchName, true, remoteRef, nil
	}

	return "", false, ref.RemoteRef{}, nil
}

type RefWithHash struct {
	Ref  ref.DoltRef
	Hash hash.Hash
}

// GetBranchesWithHashes returns all the branches in the database with their hashes
func (ddb *DoltDB) GetBranchesWithHashes(ctx context.Context) ([]RefWithHash, error) {
	var refs []RefWithHash
	err := ddb.VisitRefsOfType(ctx, branchRefFilter, func(r ref.DoltRef, addr hash.Hash) error {
		refs = append(refs, RefWithHash{r, addr})
		return nil
	})
	return refs, err
}

var allRefsFilter = map[ref.RefType]struct{}{
	ref.BranchRefType:    {},
	ref.TagRefType:       {},
	ref.WorkspaceRefType: {},
}

// GetRefsWithHashes returns the list of all commit refs in the database: tags, branches, and workspaces.
func (ddb *DoltDB) GetRefsWithHashes(ctx context.Context) ([]RefWithHash, error) {
	var refs []RefWithHash
	err := ddb.VisitRefsOfType(ctx, allRefsFilter, func(r ref.DoltRef, addr hash.Hash) error {
		refs = append(refs, RefWithHash{r, addr})
		return nil
	})
	return refs, err
}

var tagsRefFilter = map[ref.RefType]struct{}{ref.TagRefType: {}}

// GetTags returns a list of all tags in the database.
func (ddb *DoltDB) GetTags(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, tagsRefFilter)
}

// HasTag returns whether the DB has a tag with the name given
func (ddb *DoltDB) HasTag(ctx context.Context, tagName string) (string, bool, error) {
	tags, err := ddb.GetRefsOfType(ctx, tagsRefFilter)
	if err != nil {
		return "", false, err
	}

	for _, t := range tags {
		if path := t.GetPath(); strings.EqualFold(path, tagName) {
			return path, true, nil
		}
	}

	return "", false, nil
}

type TagWithHash struct {
	Tag  *Tag
	Hash hash.Hash
}

// GetTagsWithHashes returns a list of objects containing Tags with their associated Commit's hashes
func (ddb *DoltDB) GetTagsWithHashes(ctx context.Context) ([]TagWithHash, error) {
	var refs []TagWithHash
	err := ddb.VisitRefsOfType(ctx, tagsRefFilter, func(r ref.DoltRef, _ hash.Hash) error {
		if tr, ok := r.(ref.TagRef); ok {
			tag, err := ddb.ResolveTag(ctx, tr)
			if err != nil {
				return err
			}
			h, err := tag.Commit.HashOf()
			if err != nil {
				return err
			}
			refs = append(refs, TagWithHash{tag, h})
		}
		return nil
	})
	return refs, err
}

var workspacesRefFilter = map[ref.RefType]struct{}{ref.WorkspaceRefType: {}}

// GetWorkspaces returns a list of all workspaces in the database.
func (ddb *DoltDB) GetWorkspaces(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, workspacesRefFilter)
}

var remotesRefFilter = map[ref.RefType]struct{}{ref.RemoteRefType: {}}

// GetRemoteRefs returns a list of all remotes in the database.
func (ddb *DoltDB) GetRemoteRefs(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, remotesRefFilter)
}

type RemoteWithHash struct {
	Ref  ref.DoltRef
	Hash hash.Hash
}

func (ddb *DoltDB) GetRemotesWithHashes(ctx context.Context) ([]RemoteWithHash, error) {
	var refs []RemoteWithHash
	err := ddb.VisitRefsOfType(ctx, remotesRefFilter, func(r ref.DoltRef, addr hash.Hash) error {
		refs = append(refs, RemoteWithHash{r, addr})
		return nil
	})
	return refs, err
}

// GetHeadRefs returns a list of all refs that point to a Commit
func (ddb *DoltDB) GetHeadRefs(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, ref.HeadRefTypes)
}

func (ddb *DoltDB) VisitRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}, visit func(r ref.DoltRef, addr hash.Hash) error) error {
	dss, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}

	return visitDatasets(ctx, refTypeFilter, visit, dss)
}

func (ddb *DoltDB) VisitRefsOfTypeByNomsRoot(ctx context.Context, refTypeFilter map[ref.RefType]struct{}, nomsRoot hash.Hash, visit func(r ref.DoltRef, addr hash.Hash) error) error {
	dss, err := ddb.db.DatasetsByRootHash(ctx, nomsRoot)
	if err != nil {
		return err
	}

	return visitDatasets(ctx, refTypeFilter, visit, dss)
}

func visitDatasets(ctx context.Context, refTypeFilter map[ref.RefType]struct{}, visit func(r ref.DoltRef, addr hash.Hash) error, dss datas.DatasetsMap) error {
	return dss.IterAll(ctx, func(key string, addr hash.Hash) error {
		keyStr := key

		if ref.IsRef(keyStr) {
			dref, err := ref.Parse(keyStr)
			if err != nil {
				return err
			}

			if _, ok := refTypeFilter[dref.GetType()]; ok {
				err = visit(dref, addr)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// GetRefByNameInsensitive searches this Dolt database's branch, tag, and head refs for a case-insensitive
// match of the specified ref name. If a matching DoltRef is found, it is returned; otherwise an error is returned.
func (ddb *DoltDB) GetRefByNameInsensitive(ctx context.Context, refName string) (ref.DoltRef, error) {
	branchRefs, err := ddb.GetBranches(ctx)
	if err != nil {
		return nil, err
	}
	for _, branchRef := range branchRefs {
		if strings.EqualFold(branchRef.GetPath(), refName) {
			return branchRef, nil
		}
	}

	headRefs, err := ddb.GetHeadRefs(ctx)
	if err != nil {
		return nil, err
	}
	for _, headRef := range headRefs {
		if strings.EqualFold(headRef.GetPath(), refName) {
			return headRef, nil
		}
	}

	tagRefs, err := ddb.GetTags(ctx)
	if err != nil {
		return nil, err
	}
	for _, tagRef := range tagRefs {
		if strings.EqualFold(tagRef.GetPath(), refName) {
			return tagRef, nil
		}
	}

	return nil, ref.ErrInvalidRefSpec
}

func (ddb *DoltDB) GetRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}) ([]ref.DoltRef, error) {
	var refs []ref.DoltRef
	err := ddb.VisitRefsOfType(ctx, refTypeFilter, func(r ref.DoltRef, _ hash.Hash) error {
		refs = append(refs, r)
		return nil
	})
	return refs, err
}

func (ddb *DoltDB) GetRefsOfTypeByNomsRoot(ctx context.Context, refTypeFilter map[ref.RefType]struct{}, nomsRoot hash.Hash) ([]ref.DoltRef, error) {
	var refs []ref.DoltRef
	err := ddb.VisitRefsOfTypeByNomsRoot(ctx, refTypeFilter, nomsRoot, func(r ref.DoltRef, _ hash.Hash) error {
		refs = append(refs, r)
		return nil
	})
	return refs, err
}

// NewBranchAtCommit creates a new branch with HEAD at the commit given. Branch names must pass IsValidUserBranchName.
// Silently overwrites any existing branch with the same name given, if one exists.
func (ddb *DoltDB) NewBranchAtCommit(ctx context.Context, branchRef ref.DoltRef, commit *Commit, replicationStatus *ReplicationStatusController) error {
	if !IsValidBranchRef(branchRef) {
		panic(fmt.Sprintf("invalid branch name %s, use IsValidUserBranchName check", branchRef.String()))
	}

	ds, err := ddb.db.GetDataset(ctx, branchRef.String())
	if err != nil {
		return err
	}

	addr, err := commit.HashOf()
	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, addr, "")
	if err != nil {
		return err
	}

	// Update the corresponding working set at the same time, either by updating it or creating a new one
	// TODO: find all the places HEAD can change, update working set too. This is only necessary when we don't already
	//  update the working set when the head changes.
	commitRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return err
	}

	wsRef, _ := ref.WorkingSetRefForHead(branchRef)

	var ws *WorkingSet
	var currWsHash hash.Hash
	ws, err = ddb.ResolveWorkingSet(ctx, wsRef)
	if errors.Is(err, ErrWorkingSetNotFound) {
		ws = EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	} else {
		currWsHash, err = ws.HashOf()
		if err != nil {
			return err
		}
	}

	ws = ws.WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, currWsHash, TodoWorkingSetMeta(), replicationStatus)
}

// CopyWorkingSet copies a WorkingSetRef from one ref to another. If `force` is
// true, will overwrite any existing value in the destination ref. Otherwise
// will fail if the destination ref exists.
//
// If fromWSRef does not exist, this method does not return an error, but
// returns `nil`. In that case, the destination ref is left alone.
func (ddb *DoltDB) CopyWorkingSet(ctx context.Context, fromWSRef ref.WorkingSetRef, toWSRef ref.WorkingSetRef, force bool) error {
	ws, err := ddb.ResolveWorkingSet(ctx, fromWSRef)
	if err == ErrWorkingSetNotFound {
		return nil
	} else if err != nil {
		return err
	}

	var currWsHash hash.Hash
	toWS, err := ddb.ResolveWorkingSet(ctx, toWSRef)
	if err != nil && err != ErrWorkingSetNotFound {
		return err
	}
	if !force && err != ErrWorkingSetNotFound {
		return errors.New("cannot overwrite existing working set " + toWSRef.String() + " without force.")
	} else if err == nil {
		currWsHash, err = toWS.HashOf()
		if err != nil {
			return err
		}
	}

	return ddb.UpdateWorkingSet(ctx, toWSRef, ws, currWsHash, TodoWorkingSetMeta(), nil)
}

func (ddb *DoltDB) DeleteBranchWithWorkspaceCheck(ctx context.Context, branch ref.DoltRef, replicationStatus *ReplicationStatusController, wsPath string) error {
	return ddb.deleteRef(ctx, branch, replicationStatus, wsPath)
}

// DeleteBranch deletes the branch given, returning an error if it doesn't exist.
func (ddb *DoltDB) DeleteBranch(ctx context.Context, branch ref.DoltRef, replicationStatus *ReplicationStatusController) error {
	return ddb.deleteRef(ctx, branch, replicationStatus, "")
}

func (ddb *DoltDB) deleteRef(ctx context.Context, dref ref.DoltRef, replicationStatus *ReplicationStatusController, wsPath string) error {
	ds, err := ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return err
	}

	if !ds.HasHead() {
		return ErrBranchNotFound
	}

	if dref.GetType() == ref.BranchRefType {
		branches, err := ddb.GetBranches(ctx)
		if err != nil {
			return err
		}
		if len(branches) == 1 {
			return ErrCannotDeleteLastBranch
		}
	}

	_, err = ddb.db.withReplicationStatusController(replicationStatus).Delete(ctx, ds, wsPath)
	return err
}

// DeleteAllRefs Very destructive, use with caution. Not only does this drop all data, Dolt assume there is always
// a reference in the DB, so do not call this and walk away. The only use case for this method is the
// `dolt clone` command which strip everything from the remote's root object - dolt_clone stored procedure doesn't currently
// use this code path (TODO).
func (ddb *DoltDB) DeleteAllRefs(ctx context.Context) error {
	dss, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}
	err = dss.IterAll(ctx, func(key string, addr hash.Hash) error {
		ds, e := ddb.db.GetDataset(ctx, key)
		if e != nil {
			return e
		}

		_, e = ddb.db.Delete(ctx, ds, "")
		return e
	})
	return err
}

// NewTagAtCommit create a new tag at the commit given.
func (ddb *DoltDB) NewTagAtCommit(ctx context.Context, tagRef ref.DoltRef, c *Commit, meta *datas.TagMeta) error {
	if !IsValidTagRef(tagRef) {
		panic(fmt.Sprintf("invalid tag name %s, use IsValidUserTagName check", tagRef.String()))
	}

	ds, err := ddb.db.GetDataset(ctx, tagRef.String())

	if err != nil {
		return err
	}

	if ds.HasHead() {
		return fmt.Errorf("dataset already exists for tag %s", tagRef.String())
	}

	commitAddr, err := c.HashOf()
	if err != nil {
		return err
	}

	tag := datas.TagOptions{Meta: meta}

	ds, err = ddb.db.Tag(ctx, ds, commitAddr, tag)

	return err
}

// This should be used as the cancel cause for the context passed to a
// ReplicationStatusController Wait function when the wait has been canceled
// because it timed out. Seeing this error from a passed in context may be used
// by some agents to open circuit breakers or tune timeouts.
var ErrReplicationWaitFailed = errors.New("replication wait failed")

type ReplicationStatusController struct {
	// A slice of funcs which can be called to wait for the replication
	// associated with a commithook to complete. Must return if the
	// associated Context is canceled.
	Wait []func(ctx context.Context) error

	// There is an entry here for each function in Wait. If a Wait fails,
	// you can notify the corresponding function in this slice. This might
	// control resiliency behaviors like adaptive retry and timeouts,
	// circuit breakers, etc. and might feed into exposed replication
	// metrics.
	NotifyWaitFailed []func()
}

// DatabaseUpdateListener allows callbacks on a registered listener when a database is created, dropped, or when
// the working root is updated new changes are visible to other sessions.
type DatabaseUpdateListener interface {
	// WorkingRootUpdated is called when a branch working root is updated on a database and other sessions are
	// given visibility to the changes. |ctx| provides the current session information, |databaseName| indicates
	// the database being updated, and |before| and |after| are the previous and new RootValues for the working root.
	// If callers encounter any errors while processing a root update notification, they can return an error, which
	// will be logged.
	WorkingRootUpdated(ctx *sql.Context, databaseName string, branchName string, before RootValue, after RootValue) error

	// DatabaseCreated is called when a new database, named |databaseName|, has been created.
	DatabaseCreated(ctx *sql.Context, databaseName string) error

	// DatabaseDropped is called with the database named |databaseName| has been dropped.
	DatabaseDropped(ctx *sql.Context, databaseName string) error
}

var DatabaseUpdateListeners = make([]DatabaseUpdateListener, 0)

// RegisterDatabaseUpdateListener registers |listener| to receive callbacks when databases are updated.
func RegisterDatabaseUpdateListener(listener DatabaseUpdateListener) {
	DatabaseUpdateListeners = append(DatabaseUpdateListeners, listener)
}

// UpdateWorkingSet updates the working set with the ref given to the root value given
// |prevHash| is the hash of the expected WorkingSet struct stored in the ref, not the hash of the RootValue there.
func (ddb *DoltDB) UpdateWorkingSet(
	ctx context.Context,
	workingSetRef ref.WorkingSetRef,
	workingSet *WorkingSet,
	prevHash hash.Hash,
	meta *datas.WorkingSetMeta,
	replicationStatus *ReplicationStatusController,
) error {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return err
	}

	wsSpec, err := ddb.writeWorkingSet(ctx, workingSetRef, workingSet, meta, ds)
	if err != nil {
		return err
	}

	_, err = ddb.db.withReplicationStatusController(replicationStatus).UpdateWorkingSet(ctx, ds, *wsSpec, prevHash)
	return err
}

// CommitWithWorkingSet combines the functionality of CommitWithParents with UpdateWorking set, and takes a combination
// of their parameters. It's a way to update the working set and current HEAD in the same atomic transaction. It commits
// to disk a pending commit value previously created with NewPendingCommit, asserting that the working set hash given
// is still current for that HEAD.
func (ddb *DoltDB) CommitWithWorkingSet(
	ctx context.Context,
	headRef ref.DoltRef, workingSetRef ref.WorkingSetRef,
	commit *PendingCommit, workingSet *WorkingSet,
	prevHash hash.Hash,
	meta *datas.WorkingSetMeta,
	replicationStatus *ReplicationStatusController,
) (*Commit, error) {
	wsDs, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return nil, err
	}

	headDs, err := ddb.db.GetDataset(ctx, headRef.String())
	if err != nil {
		return nil, err
	}

	wsSpec, err := ddb.writeWorkingSet(ctx, workingSetRef, workingSet, meta, wsDs)
	if err != nil {
		return nil, err
	}

	commitDataset, _, err := ddb.db.withReplicationStatusController(replicationStatus).
		CommitWithWorkingSet(ctx, headDs, wsDs, commit.Roots.Staged.NomsValue(), *wsSpec, prevHash, commit.CommitOptions)
	if err != nil {
		return nil, err
	}

	commitRef, ok, err := commitDataset.MaybeHeadRef()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Commit has no head but commit succeeded. This is a bug.")
	}

	dc, err := datas.LoadCommitRef(ctx, ddb.vrw, commitRef)
	if err != nil {
		return nil, err
	}

	if dc.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	return NewCommit(ctx, ddb.vrw, ddb.ns, dc)
}

// writeWorkingSet writes the specified |workingSet| at the specified |workingSetRef| with the
// specified ws metadata, |meta|, in the dataset |wsDs| and returns the created WorkingSetSpec along with any error
// encountered. If any listeners are registered for working root updates, then they will be notified as well.
func (ddb *DoltDB) writeWorkingSet(ctx context.Context, workingSetRef ref.WorkingSetRef, workingSet *WorkingSet, meta *datas.WorkingSetMeta, wsDs datas.Dataset) (wsSpec *datas.WorkingSetSpec, err error) {
	var prevRoot RootValue
	if wsDs.HasHead() {
		prevWorkingSet, err := newWorkingSet(ctx, workingSetRef.String(), ddb.vrw, ddb.ns, wsDs)
		if err != nil {
			return nil, err
		}
		prevRoot = prevWorkingSet.workingRoot
	} else {
		// If the working set dataset doesn't have a head, then there isn't a previous root value for us to use
		// for the database update, so instead we pass in an EmptyRootValue so that implementations of
		// DatabaseUpdateListener don't have to do nil checking. This can happen when a new database is created
		// or when a new branch is created.
		prevRoot, err = EmptyRootValue(ctx, ddb.vrw, ddb.ns)
		if err != nil {
			return nil, err
		}
	}

	var branchName string
	if strings.HasPrefix(workingSet.Name, "heads/") {
		branchName = workingSet.Name[len("heads/"):]
	}

	wsSpec, err = workingSet.writeValues(ctx, ddb, meta)
	if err != nil {
		return nil, err
	}

	if branchName != "" {
		for _, listener := range DatabaseUpdateListeners {
			sqlCtx, ok := ctx.(*sql.Context)
			if ok {
				err := listener.WorkingRootUpdated(sqlCtx,
					ddb.databaseName,
					branchName,
					prevRoot,
					workingSet.WorkingRoot())
				if err != nil {
					logrus.Errorf("error notifying working root listener of update: %s", err.Error())
				}
			}
		}
	}

	return wsSpec, nil
}

// DeleteWorkingSet deletes the working set given
func (ddb *DoltDB) DeleteWorkingSet(ctx context.Context, workingSetRef ref.WorkingSetRef) error {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return err
	}

	_, err = ddb.db.Delete(ctx, ds, "")
	return err
}

func (ddb *DoltDB) DeleteTag(ctx context.Context, tag ref.DoltRef) error {
	err := ddb.deleteRef(ctx, tag, nil, "")

	if err == ErrBranchNotFound {
		return ErrTagNotFound
	}

	return err
}

// NewWorkspaceAtCommit create a new workspace at the commit given.
func (ddb *DoltDB) NewWorkspaceAtCommit(ctx context.Context, workRef ref.DoltRef, c *Commit) error {
	ds, err := ddb.db.GetDataset(ctx, workRef.String())
	if err != nil {
		return err
	}

	addr, err := c.HashOf()
	if err != nil {
		return err
	}

	ds, err = ddb.db.SetHead(ctx, ds, addr, "")

	return err
}

func (ddb *DoltDB) DeleteWorkspace(ctx context.Context, workRef ref.DoltRef) error {
	err := ddb.deleteRef(ctx, workRef, nil, "")

	if err == ErrBranchNotFound {
		return ErrWorkspaceNotFound
	}

	return err
}

// Rebase rebases the underlying db from disk, re-loading the manifest. Useful when another process might have made
// changes to the database we need to read.
func (ddb *DoltDB) Rebase(ctx context.Context) error {
	return datas.ChunkStoreFromDatabase(ddb.db).Rebase(ctx)
}

type GCMode int
const (
	GCModeDefault GCMode = iota
	GCModeFull
)

// GC performs garbage collection on this ddb.
//
// If |safepointF| is non-nil, it will be called at some point after the GC begins
// and before the GC ends. It will be called without
// Database/ValueStore/NomsBlockStore locks held. If should establish
// safepoints in every application-level in-progress read and write workflow
// against this DoltDB. Examples of doing this include, for example, blocking
// until no possibly-stale ChunkStore state is retained in memory, or failing
// certain in-progress operations which cannot be finalized in a timely manner,
// etc.
func (ddb *DoltDB) GC(ctx context.Context, mode GCMode, safepointF func() error) error {
	collector, ok := ddb.db.Database.(datas.GarbageCollector)
	if !ok {
		return fmt.Errorf("this database does not support garbage collection")
	}

	err := ddb.pruneUnreferencedDatasets(ctx)
	if err != nil {
		return err
	}

	datasets, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}

	newGen := make(hash.HashSet)
	oldGen := make(hash.HashSet)
	err = datasets.IterAll(ctx, func(keyStr string, h hash.Hash) error {
		var isOldGen bool
		switch {
		case ref.IsRef(keyStr):
			parsed, err := ref.Parse(keyStr)
			if err != nil && !errors.Is(err, ref.ErrUnknownRefType) {
				return err
			}

			refType := parsed.GetType()
			isOldGen = refType == ref.BranchRefType || refType == ref.RemoteRefType || refType == ref.InternalRefType
		}

		if isOldGen {
			oldGen.Insert(h)
		} else {
			newGen.Insert(h)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return collector.GC(ctx, oldGen, newGen, safepointF)
}

func (ddb *DoltDB) ShallowGC(ctx context.Context) error {
	return datas.PruneTableFiles(ctx, ddb.db)
}

func (ddb *DoltDB) pruneUnreferencedDatasets(ctx context.Context) error {
	dd, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}

	var deletes []string
	_ = dd.IterAll(ctx, func(dsID string, _ hash.Hash) (err error) {
		if !ref.IsRef(dsID) && !ref.IsWorkingSet(dsID) {
			deletes = append(deletes, dsID)
		}
		return nil
	})

	// e.g. flushes
	for _, dsID := range deletes {
		ds, err := ddb.db.GetDataset(ctx, dsID)
		if err != nil {
			return err
		}

		ds, err = ddb.db.Delete(ctx, ds, "")
		if err != nil {
			return err
		}

		if ds.HasHead() {
			return fmt.Errorf("unsuccessful delete for dataset %s", ds.ID())
		}
	}

	return nil
}

// PullChunks initiates a pull into this database from the source database
// given, pulling all chunks reachable from the given targetHash. Pull progress
// is communicated over the provided channel.
func (ddb *DoltDB) PullChunks(
	ctx context.Context,
	tempDir string,
	srcDB *DoltDB,
	targetHashes []hash.Hash,
	statsCh chan pull.Stats,
	skipHashes hash.HashSet,
) error {
	return pullHash(ctx, ddb.db, srcDB.db, targetHashes, tempDir, statsCh, skipHashes)
}

func pullHash(
	ctx context.Context,
	destDB, srcDB datas.Database,
	targetHashes []hash.Hash,
	tempDir string,
	statsCh chan pull.Stats,
	skipHashes hash.HashSet,
) error {
	srcCS := datas.ChunkStoreFromDatabase(srcDB)
	destCS := datas.ChunkStoreFromDatabase(destDB)
	waf := types.WalkAddrsForNBF(srcDB.Format(), skipHashes)

	if datas.CanUsePuller(srcDB) && datas.CanUsePuller(destDB) {
		puller, err := pull.NewPuller(ctx, tempDir, defaultChunksPerTF, srcCS, destCS, waf, targetHashes, statsCh)
		if err == pull.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return errors.New("Puller not supported")
	}
}

func (ddb *DoltDB) Clone(ctx context.Context, destDB *DoltDB, eventCh chan<- pull.TableFileEvent) error {
	return pull.Clone(ctx, datas.ChunkStoreFromDatabase(ddb.db), datas.ChunkStoreFromDatabase(destDB.db), eventCh)
}

// Returns |true| if the underlying ChunkStore for this DoltDB implements |chunks.TableFileStore|.
func (ddb *DoltDB) IsTableFileStore() bool {
	_, ok := datas.ChunkStoreFromDatabase(ddb.db).(chunks.TableFileStore)
	return ok
}

// ChunkJournal returns the ChunkJournal for this DoltDB, if one is in use.
func (ddb *DoltDB) ChunkJournal() *nbs.ChunkJournal {
	tableFileStore, ok := datas.ChunkStoreFromDatabase(ddb.db).(chunks.TableFileStore)
	if !ok {
		return nil
	}

	generationalNbs, ok := tableFileStore.(*nbs.GenerationalNBS)
	if !ok {
		return nil
	}

	newGen := generationalNbs.NewGen()
	nbs, ok := newGen.(*nbs.NomsBlockStore)
	if !ok {
		return nil
	}

	return nbs.ChunkJournal()
}

func (ddb *DoltDB) TableFileStoreHasJournal(ctx context.Context) (bool, error) {
	tableFileStore, ok := datas.ChunkStoreFromDatabase(ddb.db).(chunks.TableFileStore)
	if !ok {
		return false, errors.New("unsupported operation, DoltDB.TableFileStoreHasManifest on non-TableFileStore")
	}
	_, tableFiles, _, err := tableFileStore.Sources(ctx)
	if err != nil {
		return false, err
	}
	for _, tableFile := range tableFiles {
		if tableFile.FileID() == chunks.JournalFileID {
			return true, nil
		}
	}
	return false, nil
}

// DatasetsByRootHash returns the DatasetsMap for the specified root |hashof|.
func (ddb *DoltDB) DatasetsByRootHash(ctx context.Context, hashof hash.Hash) (datas.DatasetsMap, error) {
	return ddb.db.DatasetsByRootHash(ctx, hashof)
}

func (ddb *DoltDB) SetCommitHooks(ctx context.Context, postHooks []CommitHook) *DoltDB {
	ddb.db = ddb.db.SetCommitHooks(ctx, postHooks)
	return ddb
}

func (ddb *DoltDB) PrependCommitHook(ctx context.Context, hook CommitHook) *DoltDB {
	ddb.db = ddb.db.SetCommitHooks(ctx, append([]CommitHook{hook}, ddb.db.PostCommitHooks()...))
	return ddb
}

func (ddb *DoltDB) SetCommitHookLogger(ctx context.Context, wr io.Writer) *DoltDB {
	if ddb.db.Database != nil {
		ddb.db = ddb.db.SetCommitHookLogger(ctx, wr)
	}
	return ddb
}

func (ddb *DoltDB) ExecuteCommitHooks(ctx context.Context, datasetId string) error {
	ds, err := ddb.db.GetDataset(ctx, datasetId)
	if err != nil {
		return err
	}
	ddb.db.ExecuteCommitHooks(ctx, ds, false)
	return nil
}

func (ddb *DoltDB) GetBranchesByRootHash(ctx context.Context, rootHash hash.Hash) ([]RefWithHash, error) {
	dss, err := ddb.db.DatasetsByRootHash(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	var refs []RefWithHash

	err = dss.IterAll(ctx, func(key string, addr hash.Hash) error {
		keyStr := key

		var dref ref.DoltRef
		if ref.IsRef(keyStr) {
			dref, err = ref.Parse(keyStr)
			if err != nil {
				return err
			}

			if _, ok := branchRefFilter[dref.GetType()]; ok {
				refs = append(refs, RefWithHash{dref, addr})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return refs, nil
}

// AddStash takes current branch head commit, stash root value and stash metadata to create a new stash.
// It stores the new stash object in stash list Dataset, which can be created if it does not exist.
// Otherwise, it updates the stash list Dataset as there can only be one stashes Dataset.
func (ddb *DoltDB) AddStash(ctx context.Context, head *Commit, stash RootValue, meta *datas.StashMeta) error {
	stashesDS, err := ddb.db.GetDataset(ctx, ref.NewStashRef().String())
	if err != nil {
		return err
	}

	headCommitAddr, err := head.HashOf()
	if err != nil {
		return err
	}

	_, stashVal, err := ddb.writeRootValue(ctx, stash)
	if err != nil {
		return err
	}

	nbf := ddb.Format()
	vrw := ddb.ValueReadWriter()
	stashAddr, _, err := datas.NewStash(ctx, nbf, vrw, stashVal, headCommitAddr, meta)
	if err != nil {
		return err
	}

	// this either creates new stash list dataset or loads current stash list dataset if exists.
	stashList, err := datas.LoadStashList(ctx, nbf, ddb.NodeStore(), vrw, stashesDS)
	if err != nil {
		return err
	}

	stashListAddr, err := stashList.AddStash(ctx, vrw, stashAddr)
	if err != nil {
		return err
	}

	stashesDS, err = ddb.db.UpdateStashList(ctx, stashesDS, stashListAddr)
	return err
}

func (ddb *DoltDB) SetStatisics(ctx context.Context, branch string, addr hash.Hash) error {
	statsDs, err := ddb.db.GetDataset(ctx, ref.NewStatsRef(branch).String())
	if err != nil {
		return err
	}
	_, err = ddb.db.SetStatsRef(ctx, statsDs, addr)
	return err
}

func (ddb *DoltDB) DropStatisics(ctx context.Context, branch string) error {
	statsDs, err := ddb.db.GetDataset(ctx, ref.NewStatsRef(branch).String())

	_, err = ddb.db.Delete(ctx, statsDs, "")
	if err != nil {
		return err
	}
	return nil
}

var ErrNoStatistics = errors.New("no statistics found")

// GetStatistics returns the value of the singleton ref.StatsRef for this database
func (ddb *DoltDB) GetStatistics(ctx context.Context, branch string) (prolly.Map, error) {
	ds, err := ddb.db.GetDataset(ctx, ref.NewStatsRef(branch).String())
	if err != nil {
		return prolly.Map{}, err
	}

	if !ds.HasHead() {
		return prolly.Map{}, ErrNoStatistics
	}

	stats, err := datas.LoadStatistics(ctx, ddb.Format(), ddb.NodeStore(), ddb.ValueReadWriter(), ds)
	if err != nil {
		return prolly.Map{}, err
	}

	return stats.Map(), nil

}

// RemoveStashAtIdx takes and index of a stash to remove from the stash list map.
// It removes a Stash message from stash list Dataset, which cannot be performed
// by database Delete function. This function removes a single stash only and stash
// list dataset does not get removed if there are no entries left.
func (ddb *DoltDB) RemoveStashAtIdx(ctx context.Context, idx int) error {
	stashesDS, err := ddb.db.GetDataset(ctx, ref.NewStashRef().String())
	if err != nil {
		return err
	}

	if !stashesDS.HasHead() {
		return errors.New("No stash entries found.")
	}

	vrw := ddb.ValueReadWriter()
	stashList, err := datas.LoadStashList(ctx, ddb.Format(), ddb.NodeStore(), vrw, stashesDS)
	if err != nil {
		return err
	}

	stashListAddr, err := stashList.RemoveStashAtIdx(ctx, vrw, idx)
	if err != nil {
		return err
	}

	stashListCount, err := stashList.Count()
	if err != nil {
		return err
	}
	// if the stash list is empty, remove the stash list Dataset from the database
	if stashListCount == 0 {
		return ddb.RemoveAllStashes(ctx)
	}

	stashesDS, err = ddb.db.UpdateStashList(ctx, stashesDS, stashListAddr)
	return err
}

// RemoveAllStashes removes the stash list Dataset from the database,
// which equivalent to removing Stash entries from the stash list.
func (ddb *DoltDB) RemoveAllStashes(ctx context.Context) error {
	err := ddb.deleteRef(ctx, ref.NewStashRef(), nil, "")
	if err == ErrBranchNotFound {
		return nil
	}
	return err
}

// GetStashes returns array of Stash objects containing all stash entries in the stash list Dataset.
func (ddb *DoltDB) GetStashes(ctx context.Context) ([]*Stash, error) {
	stashesDS, err := ddb.db.GetDataset(ctx, ref.NewStashRef().String())
	if err != nil {
		return nil, err
	}

	if !stashesDS.HasHead() {
		return []*Stash{}, nil
	}

	return getStashList(ctx, stashesDS, ddb.vrw, ddb.NodeStore())
}

// GetStashHashAtIdx returns hash address only of the stash at given index.
func (ddb *DoltDB) GetStashHashAtIdx(ctx context.Context, idx int) (hash.Hash, error) {
	ds, err := ddb.db.GetDataset(ctx, ref.NewStashRef().String())
	if err != nil {
		return hash.Hash{}, err
	}

	if !ds.HasHead() {
		return hash.Hash{}, errors.New("No stash entries found.")
	}

	return getStashHashAtIdx(ctx, ds, ddb.NodeStore(), idx)
}

// GetStashRootAndHeadCommitAtIdx returns root value of stash working set and head commit of the branch that the stash was made on
// of the stash at given index.
func (ddb *DoltDB) GetStashRootAndHeadCommitAtIdx(ctx context.Context, idx int) (RootValue, *Commit, *datas.StashMeta, error) {
	ds, err := ddb.db.GetDataset(ctx, ref.NewStashRef().String())
	if err != nil {
		return nil, nil, nil, err
	}

	if !ds.HasHead() {
		return nil, nil, nil, errors.New("No stash entries found.")
	}

	return getStashAtIdx(ctx, ds, ddb.vrw, ddb.NodeStore(), idx)
}

// PersistGhostCommits persists the set of ghost commits to the database. This is how the application layer passes
// information about ghost commits to the storage layer. This can be called multiple times over the course of performing
// a shallow clone, but should not be called after the clone is complete.
func (ddb *DoltDB) PersistGhostCommits(ctx context.Context, ghostCommits hash.HashSet) error {
	return ddb.db.Database.PersistGhostCommitIDs(ctx, ghostCommits)
}

type FSCKReport struct {
	ChunkCount uint32
	Problems   []error
}

// FSCK performs a full file system check on the database. This is currently exposed with the CLI as `dolt fsck`
// The success of failure of the scan are returned in the report as a list of errors. The error returned by this function
// indicates a deeper issue such as having database in an old format.
func (ddb *DoltDB) FSCK(ctx context.Context, progress chan string) (*FSCKReport, error) {
	cs := datas.ChunkStoreFromDatabase(ddb.db)

	vs := types.NewValueStore(cs)

	gs, ok := cs.(*nbs.GenerationalNBS)
	if !ok {
		return nil, errors.New("FSCK requires a local database")
	}

	chunkCount, err := gs.OldGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount2, err := gs.NewGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount += chunkCount2
	proccessedCnt := int64(0)

	var errs []error

	decodeMsg := func(chk chunks.Chunk) string {
		hrs := ""
		val, err := types.DecodeValue(chk, vs)
		if err == nil {
			hrs = val.HumanReadableString()
		} else {
			hrs = fmt.Sprintf("Unable to decode value: %s", err.Error())
		}
		return hrs
	}

	// Append safely to the slice of errors with a mutex.
	errsLock := &sync.Mutex{}
	appendErr := func(err error) {
		errsLock.Lock()
		defer errsLock.Unlock()
		errs = append(errs, err)
	}

	// Callback for validating chunks. This code could be called concurrently, though that is not currently the case.
	validationCallback := func(chunk chunks.Chunk) {
		chunkOk := true
		pCnt := atomic.AddInt64(&proccessedCnt, 1)
		h := chunk.Hash()
		raw := chunk.Data()
		calcChkSum := hash.Of(raw)

		if h != calcChkSum {
			fuzzyMatch := false
			// Special case for the journal chunk source. We may have an address which has 4 null bytes at the end.
			if h[hash.ByteLen-1] == 0 && h[hash.ByteLen-2] == 0 && h[hash.ByteLen-3] == 0 && h[hash.ByteLen-4] == 0 {
				// Now we'll just verify that the first 16 bytes match.
				ln := hash.ByteLen - 4
				fuzzyMatch = bytes.Compare(h[:ln], calcChkSum[:ln]) == 0
			}
			if !fuzzyMatch {
				hrs := decodeMsg(chunk)
				appendErr(errors.New(fmt.Sprintf("Chunk: %s content hash mismatch: %s\n%s", h.String(), calcChkSum.String(), hrs)))
				chunkOk = false
			}
		}

		if chunkOk {
			// Round trip validation. Ensure that the top level store returns the same data.
			c, err := cs.Get(ctx, h)
			if err != nil {
				appendErr(errors.New(fmt.Sprintf("Chunk: %s load failed with error: %s", h.String(), err.Error())))
				chunkOk = false
			} else if bytes.Compare(raw, c.Data()) != 0 {
				hrs := decodeMsg(chunk)
				appendErr(errors.New(fmt.Sprintf("Chunk: %s read with incorrect ID: %s\n%s", h.String(), c.Hash().String(), hrs)))
				chunkOk = false
			}
		}

		percentage := (float64(pCnt) * 100) / float64(chunkCount)
		result := fmt.Sprintf("(%4.1f%% done)", percentage)

		progStr := "OK: " + h.String()
		if !chunkOk {
			progStr = "FAIL: " + h.String()
		}
		progStr = result + " " + progStr
		progress <- progStr
	}

	err = gs.OldGen().IterateAllChunks(ctx, validationCallback)
	if err != nil {
		return nil, err
	}
	err = gs.NewGen().IterateAllChunks(ctx, validationCallback)
	if err != nil {
		return nil, err
	}

	FSCKReport := FSCKReport{Problems: errs, ChunkCount: chunkCount}

	return &FSCKReport, nil
}
