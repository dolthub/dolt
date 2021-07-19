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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

func init() {
	types.CreateEditAccForMapEdits = func(nbf *types.NomsBinFormat) types.EditAccumulator {
		return edits.NewAsyncSortedEdits(nbf, 16*1024, 4, 2)
	}
}

const (
	creationBranch   = "create"
	MasterBranch     = "master"
	CommitStructName = "Commit"

	defaultChunksPerTF = 256 * 1024
)

// LocalDirDoltDB stores the db in the current directory
var LocalDirDoltDB = "file://./" + dbfactory.DoltDataDir

// InMemDoltDB stores the DoltDB db in memory and is primarily used for testing
var InMemDoltDB = "mem://"

var ErrNoRootValAtHash = errors.New("there is no dolt root value at that hash")
var ErrCannotDeleteLastBranch = errors.New("cannot delete the last branch")

// DoltDB wraps access to the underlying noms database and hides some of the details of the underlying storage.
// Additionally the noms codebase uses panics in a way that is non idiomatic and We've opted to recover and return
// errors in many cases.
type DoltDB struct {
	db datas.Database
}

// DoltDBFromCS creates a DoltDB from a noms chunks.ChunkStore
func DoltDBFromCS(cs chunks.ChunkStore) *DoltDB {
	db := datas.NewDatabase(cs)

	return &DoltDB{db}
}

// LoadDoltDB will acquire a reference to the underlying noms db.  If the Location is InMemDoltDB then a reference
// to a newly created in memory database will be used. If the location is LocalDirDoltDB, the directory must exist or
// this returns nil.
func LoadDoltDB(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, fs filesys.Filesys) (*DoltDB, error) {
	return LoadDoltDBWithParams(ctx, nbf, urlStr, fs, nil)
}

func LoadDoltDBWithParams(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, fs filesys.Filesys, params map[string]string) (*DoltDB, error) {
	if urlStr == LocalDirDoltDB {
		exists, isDir := fs.Exists(dbfactory.DoltDataDir)

		if !exists {
			return nil, errors.New("missing dolt data directory")
		} else if !isDir {
			return nil, errors.New("file exists where the dolt data directory should be")
		}

		absPath, err := fs.Abs(dbfactory.DoltDataDir)
		if err != nil {
			return nil, err
		}

		urlStr = fmt.Sprintf("file://%s", filepath.ToSlash(absPath))
	}

	db, err := dbfactory.CreateDB(ctx, nbf, urlStr, params)

	if err != nil {
		return nil, err
	}

	return &DoltDB{db}, nil
}

func (ddb *DoltDB) CSMetricsSummary() string {
	return datas.GetCSStatSummaryForDB(ddb.db)
}

// WriteEmptyRepo will create initialize the given db with a master branch which points to a commit which has valid
// metadata for the creation commit, and an empty RootValue.
func (ddb *DoltDB) WriteEmptyRepo(ctx context.Context, name, email string) error {
	return ddb.WriteEmptyRepoWithCommitTime(ctx, name, email, CommitNowFunc())
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitTime(ctx context.Context, name, email string, t time.Time) error {
	// precondition checks
	name = strings.TrimSpace(name)
	email = strings.TrimSpace(email)

	if name == "" || email == "" {
		panic("Passed bad name or email.  Both should be valid")
	}

	ds, err := ddb.db.GetDataset(ctx, creationBranch)

	if err != nil {
		return err
	}

	if ds.HasHead() {
		return errors.New("database already exists")
	}

	rv, err := EmptyRootValue(ctx, ddb.db)

	if err != nil {
		return err
	}

	_, err = ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	cm, _ := NewCommitMetaWithUserTS(name, email, "Initialize data repository", t)

	parents, err := types.NewList(ctx, ddb.db)
	if err != nil {
		return err
	}

	meta, err := cm.toNomsStruct(ddb.db.Format())

	if err != nil {
		return err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: meta, Policy: nil}

	dref := ref.NewInternalRef(creationBranch)
	ds, err = ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return err
	}

	firstCommit, err := ddb.db.Commit(ctx, ds, rv.valueSt, commitOpts)

	if err != nil {
		return err
	}

	dref = ref.NewBranchRef(MasterBranch)
	ds, err = ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return err
	}

	headRef, ok, err := firstCommit.MaybeHeadRef()

	if err != nil {
		return err
	}

	if !ok {
		return errors.New("commit without head")
	}

	_, err = ddb.db.SetHead(ctx, ds, headRef)

	return err
}

func getCommitStForRefStr(ctx context.Context, db datas.Database, ref string) (types.Struct, error) {
	if !datas.DatasetFullRe.MatchString(ref) {
		return types.EmptyStruct(db.Format()), fmt.Errorf("invalid ref format: %s", ref)
	}

	ds, err := db.GetDataset(ctx, ref)

	if err != nil {
		return types.EmptyStruct(db.Format()), err
	}

	dsHead, hasHead := ds.MaybeHead()

	if !hasHead {
		return types.EmptyStruct(db.Format()), ErrBranchNotFound
	}

	if dsHead.Name() == datas.CommitName {
		return dsHead, nil
	}

	if dsHead.Name() == datas.TagName {
		commitRef, ok, err := dsHead.MaybeGet(datas.TagCommitRefField)
		if err != nil {
			return types.EmptyStruct(db.Format()), err
		}
		if !ok {
			err = fmt.Errorf("tag struct does not have field %s", datas.TagCommitRefField)
			return types.EmptyStruct(db.Format()), err
		}

		commitSt, err := commitRef.(types.Ref).TargetValue(ctx, db)
		if err != nil {
			return types.EmptyStruct(db.Format()), err
		}

		return commitSt.(types.Struct), nil
	}

	err = fmt.Errorf("dataset head is neither commit nor tag")
	return types.EmptyStruct(db.Format()), err
}

func getCommitStForHash(ctx context.Context, db datas.Database, c string) (types.Struct, error) {
	prefixed := c

	if !strings.HasPrefix(c, "#") {
		prefixed = "#" + c
	}

	ap, err := spec.NewAbsolutePath(prefixed)

	if err != nil {
		return types.EmptyStruct(db.Format()), err
	}

	val := ap.Resolve(ctx, db)

	if val == nil {
		return types.EmptyStruct(db.Format()), ErrHashNotFound
	}

	valSt, ok := val.(types.Struct)

	if !ok || valSt.Name() != CommitStructName {
		return types.EmptyStruct(db.Format()), ErrFoundHashNotACommit
	}

	return valSt, nil
}

func getAncestor(ctx context.Context, vrw types.ValueReadWriter, commitSt types.Struct, aSpec *AncestorSpec) (types.Struct, error) {
	if aSpec == nil || len(aSpec.Instructions) == 0 {
		return commitSt, nil
	}

	instructions := aSpec.Instructions
	for _, inst := range instructions {
		cm := NewCommit(vrw, commitSt)

		numPars, err := cm.NumParents()

		if err != nil {
			return types.EmptyStruct(vrw.Format()), err
		}

		if inst < numPars {
			commitStPtr, err := cm.getParent(ctx, inst)

			if err != nil {
				return types.EmptyStruct(vrw.Format()), err
			}

			if commitStPtr == nil {
				return types.EmptyStruct(vrw.Format()), ErrInvalidAncestorSpec
			}
			commitSt = *commitStPtr
		} else {
			return types.EmptyStruct(vrw.Format()), ErrInvalidAncestorSpec
		}
	}

	return commitSt, nil
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
	Head    *RootValue
	Working *RootValue
	Staged  *RootValue
}

// Resolve takes a CommitSpec and returns a Commit, or an error if the commit cannot be found.
// If the CommitSpec is HEAD, Resolve also needs the DoltRef of the current working branch.
func (ddb *DoltDB) Resolve(ctx context.Context, cs *CommitSpec, cwb ref.DoltRef) (*Commit, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	var commitSt types.Struct
	var err error
	switch cs.csType {
	case hashCommitSpec:
		commitSt, err = getCommitStForHash(ctx, ddb.db, cs.baseSpec)
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
			commitSt, err = getCommitStForRefStr(ctx, ddb.db, candidate)
			if err == nil {
				break
			}
			if err != ErrBranchNotFound {
				return nil, err
			}
		}
	case headCommitSpec:
		if cwb == nil {
			return nil, fmt.Errorf("cannot use a nil current working branch with a HEAD commit spec")
		}
		commitSt, err = getCommitStForRefStr(ctx, ddb.db, cwb.String())
	default:
		panic("unrecognized commit spec csType: " + cs.csType)
	}

	if err != nil {
		return nil, err
	}

	commitSt, err = getAncestor(ctx, ddb.db, commitSt, cs.aSpec)

	if err != nil {
		return nil, err
	}

	return NewCommit(ddb.db, commitSt), nil
}

// ResolveCommitRef takes a DoltRef and returns a Commit, or an error if the commit cannot be found. The ref given must
// point to a Commit.
func (ddb *DoltDB) ResolveCommitRef(ctx context.Context, ref ref.DoltRef) (*Commit, error) {
	commitSt, err := getCommitStForRefStr(ctx, ddb.db, ref.String())
	if err != nil {
		return nil, err
	}
	return NewCommit(ddb.db, commitSt), nil
}

// ResolveTag takes a TagRef and returns the corresponding Tag object.
func (ddb *DoltDB) ResolveTag(ctx context.Context, tagRef ref.TagRef) (*Tag, error) {
	ds, err := ddb.db.GetDataset(ctx, tagRef.String())

	if err != nil {
		return nil, ErrTagNotFound
	}

	tagSt, hasHead := ds.MaybeHead()

	if !hasHead {
		return nil, ErrTagNotFound
	}

	if tagSt.Name() != datas.TagName {
		return nil, fmt.Errorf("tagRef head is not a tag")
	}

	return NewTag(ctx, tagRef.GetPath(), ddb.db, tagSt)
}

// ResolveWorkingSet takes a WorkingSetRef and returns the corresponding WorkingSet object.
func (ddb *DoltDB) ResolveWorkingSet(ctx context.Context, workingSetRef ref.WorkingSetRef) (*WorkingSet, error) {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())

	if err != nil {
		return nil, ErrWorkingSetNotFound
	}

	wsSt, hasHead := ds.MaybeHead()

	if !hasHead {
		return nil, ErrWorkingSetNotFound
	}

	if wsSt.Name() != datas.WorkingSetName {
		return nil, fmt.Errorf("workingSetRef head is not a workingSetRef")
	}

	return NewWorkingSet(ctx, workingSetRef.GetPath(), ddb.db, wsSt)
}

// TODO: convenience method to resolve the head commit of a branch.

// WriteRootValue will write a doltdb.RootValue instance to the database.  This value will not be associated with a commit
// and can be committed by hash at a later time.  Returns the hash of the value written.
// This method is the primary place in doltcore that handles setting the FeatureVersion of root values to the current
// value, so all writes of RootValues should happen here.
func (ddb *DoltDB) WriteRootValue(ctx context.Context, rv *RootValue) (hash.Hash, error) {
	valRef, err := ddb.writeRootValue(ctx, rv)
	if err != nil {
		return hash.Hash{}, err
	}

	err = ddb.db.Flush(ctx)
	if err != nil {
		return hash.Hash{}, err
	}

	return valRef.TargetHash(), nil
}

// writeRootValue writes the root value given to the DB and returns a ref to it. Unlike WriteRootValue, this method
// does not flush the DB to disk afterward.
// This method is the primary place in doltcore that handles setting the FeatureVersion of root values to the current
// value, so all writes of RootValues should happen here or via WriteRootValue.
func (ddb *DoltDB) writeRootValue(ctx context.Context, rv *RootValue) (types.Ref, error) {
	var err error
	rv.valueSt, err = rv.valueSt.Set(featureVersKey, types.Int(DoltFeatureVersion))
	if err != nil {
		return types.Ref{}, err
	}

	return ddb.db.WriteValue(ctx, rv.valueSt)
}

// ReadRootValue reads the RootValue associated with the hash given and returns it. Returns an error if the value cannot
// be read, or if the hash given doesn't represent a dolt RootValue.
func (ddb *DoltDB) ReadRootValue(ctx context.Context, h hash.Hash) (*RootValue, error) {
	val, err := ddb.db.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrNoRootValAtHash
	}

	rootSt, ok := val.(types.Struct)
	if !ok || rootSt.Name() != ddbRootStructName {
		return nil, ErrNoRootValAtHash
	}

	return newRootValue(ddb.db, rootSt)
}

// Commit will update a branch's head value to be that of a previously committed root value hash
func (ddb *DoltDB) Commit(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, cm *CommitMeta) (*Commit, error) {
	if dref.GetType() != ref.BranchRefType {
		panic("can't commit to ref that isn't branch atm.  will probably remove this.")
	}

	return ddb.CommitWithParentSpecs(ctx, valHash, dref, nil, cm)
}

// FastForward fast-forwards the branch given to the commit given.
func (ddb *DoltDB) FastForward(ctx context.Context, branch ref.DoltRef, commit *Commit) error {
	ds, err := ddb.db.GetDataset(ctx, branch.String())

	if err != nil {
		return err
	}

	rf, err := types.NewRef(commit.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

	_, err = ddb.db.FastForward(ctx, ds, rf)

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

	stRef, err := types.NewRef(cm.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

	return ddb.SetHead(ctx, ref, stRef)
}

func (ddb *DoltDB) SetHead(ctx context.Context, ref ref.DoltRef, stRef types.Ref) error {
	ds, err := ddb.db.GetDataset(ctx, ref.String())

	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, stRef)
	return err
}

// CommitWithParentSpecs commits the value hash given to the branch given, using the list of parent hashes given. Returns an
// error if the value or any parents can't be resolved, or if anything goes wrong accessing the underlying storage.
func (ddb *DoltDB) CommitWithParentSpecs(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, parentCmSpecs []*CommitSpec, cm *CommitMeta) (*Commit, error) {
	var parentCommits []*Commit
	for _, parentCmSpec := range parentCmSpecs {
		cm, err := ddb.Resolve(ctx, parentCmSpec, nil)

		if err != nil {
			return nil, err
		}
		parentCommits = append(parentCommits, cm)
	}
	return ddb.CommitWithParentCommits(ctx, valHash, dref, parentCommits, cm)
}

func (ddb *DoltDB) CommitWithParentCommits(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, parentCommits []*Commit, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	val, err := ddb.db.ReadValue(ctx, valHash)

	if err != nil {
		return nil, err
	}

	if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	ds, err := ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return nil, err
	}

	l, err := types.NewList(ctx, ddb.db)

	if err != nil {
		return nil, err
	}

	parentEditor := l.Edit()

	headRef, hasHead, err := ds.MaybeHeadRef()

	if err != nil {
		return nil, err
	}

	if hasHead {
		parentEditor = parentEditor.Append(headRef)
	}

	for _, cm := range parentCommits {
		rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

		if err != nil {
			return nil, err
		}

		parentEditor = parentEditor.Append(rf)
	}

	parents, err := parentEditor.List(ctx)

	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.db.Format())

	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: st, Policy: nil}
	ds, err = ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return nil, err
	}

	ds, err = ddb.db.Commit(ctx, ds, val, commitOpts)

	if err != nil {
		return nil, err
	}

	var ok bool
	commitSt, ok = ds.MaybeHead()
	if !ok {
		return nil, errors.New("commit has no head but commit succeeded (How?!?!?)")
	}

	return NewCommit(ddb.db, commitSt), nil
}

// dangling commits are unreferenced by any branch or ref. They are created in the course of programmatic updates
// such as rebase. You must create a ref to a dangling commit for it to be reachable
func (ddb *DoltDB) CommitDanglingWithParentCommits(ctx context.Context, valHash hash.Hash, parentCommits []*Commit, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	val, err := ddb.db.ReadValue(ctx, valHash)
	if err != nil {
		return nil, err
	}
	if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	l, err := types.NewList(ctx, ddb.db)
	if err != nil {
		return nil, err
	}

	parentEditor := l.Edit()

	for _, cm := range parentCommits {
		rf, err := types.NewRef(cm.commitSt, ddb.db.Format())
		if err != nil {
			return nil, err
		}

		parentEditor = parentEditor.Append(rf)
	}

	parents, err := parentEditor.List(ctx)
	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.db.Format())
	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: st, Policy: nil}
	commitSt, err = ddb.db.CommitDangling(ctx, val, commitOpts)
	if err != nil {
		return nil, err
	}

	return NewCommit(ddb.db, commitSt), nil
}

// ValueReadWriter returns the underlying noms database as a types.ValueReadWriter.
func (ddb *DoltDB) ValueReadWriter() types.ValueReadWriter {
	return ddb.db
}

func (ddb *DoltDB) Format() *types.NomsBinFormat {
	return ddb.db.Format()
}

func WriteValAndGetRef(ctx context.Context, vrw types.ValueReadWriter, val types.Value) (types.Ref, error) {
	valRef, err := types.NewRef(val, vrw.Format())

	if err != nil {
		return types.Ref{}, err
	}

	targetVal, err := valRef.TargetValue(ctx, vrw)

	if err != nil {
		return types.Ref{}, err
	}

	if targetVal == nil {
		_, err = vrw.WriteValue(ctx, val)

		if err != nil {
			return types.Ref{}, err
		}
	}

	return valRef, err
}

// ResolveParent returns the n-th ancestor of a given commit (direct parent is index 0). error return value will be
// non-nil in the case that the commit cannot be resolved, there aren't as many ancestors as requested, or the
// underlying storage cannot be accessed.
func (ddb *DoltDB) ResolveParent(ctx context.Context, commit *Commit, parentIdx int) (*Commit, error) {
	parentCommitSt, err := commit.getParent(ctx, parentIdx)
	if err != nil {
		return nil, err
	}
	return NewCommit(ddb.ValueReadWriter(), *parentCommitSt), nil
}

func (ddb *DoltDB) ResolveAllParents(ctx context.Context, commit *Commit) ([]*Commit, error) {
	num, err := commit.NumParents()
	if err != nil {
		return nil, err
	}
	resolved := make([]*Commit, num)
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
	dss, err := ddb.db.Datasets(ctx)

	if err != nil {
		return false, err
	}

	return dss.Has(ctx, types.String(doltRef.String()))
}

var branchRefFilter = map[ref.RefType]struct{}{ref.BranchRefType: {}}

// GetBranches returns a list of all branches in the database.
func (ddb *DoltDB) GetBranches(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, branchRefFilter)
}

type BranchWithHash struct {
	Ref  ref.DoltRef
	Hash hash.Hash
}

func (ddb *DoltDB) GetBranchesWithHashes(ctx context.Context) ([]BranchWithHash, error) {
	var refs []BranchWithHash
	err := ddb.VisitRefsOfType(ctx, branchRefFilter, func(r ref.DoltRef, v types.Value) error {
		if tr, ok := v.(types.Ref); ok {
			refs = append(refs, BranchWithHash{r, tr.TargetHash()})
		}
		return nil
	})
	return refs, err
}

var tagsRefFilter = map[ref.RefType]struct{}{ref.TagRefType: {}}

// GetTags returns a list of all tags in the database.
func (ddb *DoltDB) GetTags(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, tagsRefFilter)
}

var workspacesRefFilter = map[ref.RefType]struct{}{ref.WorkspaceRefType: {}}

// GetWorkspaces returns a list of all workspaces in the database.
func (ddb *DoltDB) GetWorkspaces(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, workspacesRefFilter)
}

// GetHeadRefs returns a list of all refs that point to a Commit
func (ddb *DoltDB) GetHeadRefs(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, ref.HeadRefTypes)
}

func (ddb *DoltDB) VisitRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}, visit func(r ref.DoltRef, v types.Value) error) error {
	dss, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}

	return dss.IterAll(ctx, func(key, v types.Value) error {
		keyStr := string(key.(types.String))

		var dref ref.DoltRef
		if ref.IsRef(keyStr) {
			dref, err = ref.Parse(keyStr)
			if err != nil {
				return err
			}

			if _, ok := refTypeFilter[dref.GetType()]; ok {
				err = visit(dref, v)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (ddb *DoltDB) GetRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}) ([]ref.DoltRef, error) {
	var refs []ref.DoltRef
	err := ddb.VisitRefsOfType(ctx, refTypeFilter, func(r ref.DoltRef, v types.Value) error {
		refs = append(refs, r)
		return nil
	})
	return refs, err
}

// NewBranchAtCommit creates a new branch with HEAD at the commit given. Branch names must pass IsValidUserBranchName.
func (ddb *DoltDB) NewBranchAtCommit(ctx context.Context, branchRef ref.DoltRef, commit *Commit) error {
	if !IsValidBranchRef(branchRef) {
		panic(fmt.Sprintf("invalid branch name %s, use IsValidUserBranchName check", branchRef.String()))
	}

	ds, err := ddb.db.GetDataset(ctx, branchRef.String())
	if err != nil {
		return err
	}

	rf, err := types.NewRef(commit.commitSt, ddb.db.Format())
	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, rf)
	if err != nil {
		return err
	}

	// Update the corresponding working set at the same time, either by updating it or creating a new one
	// TODO: find all the places HEAD can change, update working set too. This is only necessary when we don't already
	//  update the working set when the head changes.
	commitRoot, err := commit.GetRootValue()
	wsRef, _ := ref.WorkingSetRefForHead(branchRef)

	var ws *WorkingSet
	var currWsHash hash.Hash
	ws, err = ddb.ResolveWorkingSet(ctx, wsRef)
	if err == ErrWorkingSetNotFound {
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
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, currWsHash, nil)
}

// DeleteBranch deletes the branch given, returning an error if it doesn't exist.
func (ddb *DoltDB) DeleteBranch(ctx context.Context, branch ref.DoltRef) error {
	return ddb.deleteRef(ctx, branch)
}

func (ddb *DoltDB) deleteRef(ctx context.Context, dref ref.DoltRef) error {
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

	_, err = ddb.db.Delete(ctx, ds)
	return err
}

// NewTagAtCommit create a new tag at the commit given.
func (ddb *DoltDB) NewTagAtCommit(ctx context.Context, tagRef ref.DoltRef, c *Commit, meta *TagMeta) error {
	if !IsValidTagRef(tagRef) {
		panic(fmt.Sprintf("invalid tag name %s, use IsValidUserTagName check", tagRef.String()))
	}

	ds, err := ddb.db.GetDataset(ctx, tagRef.String())

	if err != nil {
		return err
	}

	_, hasHead, err := ds.MaybeHeadRef()

	if err != nil {
		return err
	}
	if hasHead {
		return fmt.Errorf("dataset already exists for tag %s", tagRef.String())
	}

	r, err := types.NewRef(c.commitSt, ddb.Format())

	if err != nil {
		return err
	}

	st, err := meta.toNomsStruct(ddb.db.Format())

	if err != nil {
		return err
	}

	tag := datas.TagOptions{Meta: st}

	ds, err = ddb.db.Tag(ctx, ds, r, tag)

	return err
}

// UpdateWorkingSet updates the working set with the ref given to the root value given
// |prevHash| is the hash of the expected WorkingSet struct stored in the ref, not the hash of the RootValue there.
func (ddb *DoltDB) UpdateWorkingSet(
	ctx context.Context,
	workingSetRef ref.WorkingSetRef,
	workingSet *WorkingSet,
	prevHash hash.Hash,
	meta *WorkingSetMeta,
) error {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return err
	}

	// logrus.Tracef("Updating working set with root %s", workingSet.RootValue().DebugString(ctx, true))

	workingRootRef, stagedRef, mergeStateRef, err := workingSet.writeValues(ctx, ddb)
	if err != nil {
		return err
	}

	// While we still have places that need user info threaded through, we're lenient on providing the meta
	var metaSt types.Struct
	if meta != nil {
		metaSt, err = meta.toNomsStruct(types.Format_Default)
		if err != nil {
			return err
		}
	} else {
		metaSt, err = datas.NewWorkingSetMeta(types.Format_Default, "incomplete", "incomplete", uint64(time.Now().Unix()), "incomplete")
		if err != nil {
			return err
		}
	}

	_, err = ddb.db.UpdateWorkingSet(ctx, ds, datas.WorkingSetSpec{
		Meta:        datas.WorkingSetMeta{Meta: metaSt},
		WorkingRoot: workingRootRef,
		StagedRoot:  stagedRef,
		MergeState:  mergeStateRef,
	}, prevHash)

	return err
}

// DeleteWorkingSet deletes the working set given
func (ddb *DoltDB) DeleteWorkingSet(ctx context.Context, workingSetRef ref.WorkingSetRef) error {
	ds, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return err
	}

	_, err = ddb.db.Delete(ctx, ds)
	return err
}

func (ddb *DoltDB) DeleteTag(ctx context.Context, tag ref.DoltRef) error {
	err := ddb.deleteRef(ctx, tag)

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

	r, err := types.NewRef(c.commitSt, ddb.Format())
	if err != nil {
		return err
	}

	ds, err = ddb.db.SetHead(ctx, ds, r)

	return err
}

func (ddb *DoltDB) DeleteWorkspace(ctx context.Context, workRef ref.DoltRef) error {
	err := ddb.deleteRef(ctx, workRef)

	if err == ErrBranchNotFound {
		return ErrWorkspaceNotFound
	}

	return err
}

// GC performs garbage collection on this ddb. Values passed in |uncommitedVals| will be temporarily saved during gc.
func (ddb *DoltDB) GC(ctx context.Context, uncommitedVals ...hash.Hash) error {
	collector, ok := ddb.db.(datas.GarbageCollector)
	if !ok {
		return fmt.Errorf("this database does not support garbage collection")
	}

	err := ddb.pruneUnreferencedDatasets(ctx)
	if err != nil {
		return err
	}

	rand.Seed(time.Now().UnixNano())
	tmpDatasets := make([]datas.Dataset, len(uncommitedVals))
	for i, h := range uncommitedVals {
		v, err := ddb.db.ReadValue(ctx, h)
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("empty value for value hash %s", h.String())
		}

		ds, err := ddb.db.GetDataset(ctx, fmt.Sprintf("tmp/%d", rand.Int63()))
		if err != nil {
			return err
		}

		r, err := WriteValAndGetRef(ctx, ddb.db, v)
		if err != nil {
			return err
		}

		ds, err = ddb.db.CommitValue(ctx, ds, r)
		if err != nil {
			return err
		}
		if !ds.HasHead() {
			return fmt.Errorf("could not save value %s", h.String())
		}

		tmpDatasets[i] = ds
	}

	err = collector.GC(ctx)
	if err != nil {
		return err
	}

	for _, ds := range tmpDatasets {
		ds, err = ddb.db.Delete(ctx, ds)
		if err != nil {
			return err
		}

		if ds.HasHead() {
			return fmt.Errorf("unsuccessful delete for dataset %s", ds.ID())
		}
	}

	return nil
}

func (ddb *DoltDB) pruneUnreferencedDatasets(ctx context.Context) error {
	dd, err := ddb.db.Datasets(ctx)
	if err != nil {
		return err
	}

	var deletes []string
	_ = dd.Iter(ctx, func(ds, _ types.Value) (stop bool, err error) {
		dsID := string(ds.(types.String))
		if !ref.IsRef(dsID) && !ref.IsWorkingSet(dsID) {
			deletes = append(deletes, dsID)
		}
		return false, nil
	})

	// e.g. flushes
	for _, dsID := range deletes {
		ds, err := ddb.db.GetDataset(ctx, dsID)
		if err != nil {
			return err
		}

		ds, err = ddb.db.Delete(ctx, ds)
		if err != nil {
			return err
		}

		if ds.HasHead() {
			return fmt.Errorf("unsuccessful delete for dataset %s", ds.ID())
		}
	}

	return nil
}

// PushChunks initiates a push into a database from the source database given, at the Value ref given. Pull progress is
// communicated over the provided channel.
func (ddb *DoltDB) PushChunks(ctx context.Context, tempDir string, srcDB *DoltDB, rf types.Ref, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	if datas.CanUsePuller(srcDB.db) && datas.CanUsePuller(ddb.db) {
		puller, err := datas.NewPuller(ctx, tempDir, defaultChunksPerTF, srcDB.db, ddb.db, rf.TargetHash(), pullerEventCh)

		if err == datas.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return datas.Pull(ctx, srcDB.db, ddb.db, rf, progChan)
	}
}

func (ddb *DoltDB) PushChunksForRefHash(ctx context.Context, tempDir string, srcDB *DoltDB, h hash.Hash, pullerEventCh chan datas.PullerEvent) error {
	if datas.CanUsePuller(srcDB.db) && datas.CanUsePuller(ddb.db) {
		puller, err := datas.NewPuller(ctx, tempDir, defaultChunksPerTF, srcDB.db, ddb.db, h, pullerEventCh)

		if err == datas.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return errors.New("this type of chunk store does not support this operation")
	}
}

// PullChunks initiates a pull into a database from the source database given, at the commit given. Progress is
// communicated over the provided channel.
func (ddb *DoltDB) PullChunks(ctx context.Context, tempDir string, srcDB *DoltDB, stRef types.Ref, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	if datas.CanUsePuller(srcDB.db) && datas.CanUsePuller(ddb.db) {
		puller, err := datas.NewPuller(ctx, tempDir, defaultChunksPerTF, srcDB.db, ddb.db, stRef.TargetHash(), pullerEventCh)

		if err == datas.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return datas.PullWithoutBatching(ctx, srcDB.db, ddb.db, stRef, progChan)
	}
}

func (ddb *DoltDB) Clone(ctx context.Context, destDB *DoltDB, eventCh chan<- datas.TableFileEvent) error {
	return datas.Clone(ctx, ddb.db, destDB.db, eventCh)
}

func (ddb *DoltDB) GetStorageVersion(ctx context.Context) (string, error) {
	return datas.GetManifestStorageVersion(ctx, ddb.db)
}
