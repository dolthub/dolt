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
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/types/edits"
)

func init() {
	types.CreateEditAccForMapEdits = edits.NewAsyncSortedEditsWithDefaults
}

const (
	creationBranch = "create"

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
	db  hooksDatabase
	vrw types.ValueReadWriter
}

// DoltDBFromCS creates a DoltDB from a noms chunks.ChunkStore
func DoltDBFromCS(cs chunks.ChunkStore) *DoltDB {
	vrw := types.NewValueStore(cs)
	db := datas.NewTypesDatabase(vrw)

	return &DoltDB{hooksDatabase{Database: db}, vrw}
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

	db, vrw, err := dbfactory.CreateDB(ctx, nbf, urlStr, params)

	if err != nil {
		return nil, err
	}

	return &DoltDB{hooksDatabase{Database: db}, vrw}, nil
}

// NomsRoot returns the hash of the noms dataset map
func (ddb *DoltDB) NomsRoot(ctx context.Context) (hash.Hash, error) {
	return datas.ChunkStoreFromDatabase(ddb.db).Root(ctx)
}

// CommitRoot executes a chunkStore commit, atomically swapping the root hash of the database manifest
func (ddb *DoltDB) CommitRoot(ctx context.Context, last, current hash.Hash) (bool, error) {
	return datas.ChunkStoreFromDatabase(ddb.db).Commit(ctx, last, current)
}

func (ddb *DoltDB) CSMetricsSummary() string {
	return datas.GetCSStatSummaryForDB(ddb.db)
}

// WriteEmptyRepo will create initialize the given db with a master branch which points to a commit which has valid
// metadata for the creation commit, and an empty RootValue.
func (ddb *DoltDB) WriteEmptyRepo(ctx context.Context, initBranch, name, email string) error {
	return ddb.WriteEmptyRepoWithCommitTime(ctx, initBranch, name, email, CommitNowFunc())
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitTime(ctx context.Context, initBranch, name, email string, t time.Time) error {
	return ddb.WriteEmptyRepoWithCommitTimeAndDefaultBranch(ctx, name, email, t, ref.NewBranchRef(initBranch))
}

func (ddb *DoltDB) WriteEmptyRepoWithCommitTimeAndDefaultBranch(
	ctx context.Context,
	name, email string,
	t time.Time,
	init ref.BranchRef,
) error {
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

	rv, err := EmptyRootValue(ctx, ddb.vrw)

	if err != nil {
		return err
	}

	_, err = ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	cm, _ := NewCommitMetaWithUserTS(name, email, "Initialize data repository", t)

	parents, err := types.NewList(ctx, ddb.vrw)
	if err != nil {
		return err
	}

	meta, err := cm.toNomsStruct(ddb.vrw.Format())

	if err != nil {
		return err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: meta}

	cb := ref.NewInternalRef(creationBranch)
	ds, err = ddb.db.GetDataset(ctx, cb.String())

	if err != nil {
		return err
	}

	firstCommit, err := ddb.db.Commit(ctx, ds, rv.valueSt, commitOpts)

	if err != nil {
		return err
	}

	ds, err = ddb.db.GetDataset(ctx, init.String())

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

func getCommitStForRefStr(ctx context.Context, db datas.Database, vrw types.ValueReadWriter, ref string) (types.Struct, error) {
	if !datas.DatasetFullRe.MatchString(ref) {
		return types.Struct{}, fmt.Errorf("invalid ref format: %s", ref)
	}

	ds, err := db.GetDataset(ctx, ref)

	if err != nil {
		return types.Struct{}, err
	}

	dsHead, hasHead := ds.MaybeHead()

	if !hasHead {
		return types.Struct{}, ErrBranchNotFound
	}

	if dsHead.Name() == datas.CommitName {
		return dsHead, nil
	}

	if dsHead.Name() == datas.TagName {
		commitRef, ok, err := dsHead.MaybeGet(datas.TagCommitRefField)
		if err != nil {
			return types.Struct{}, err
		}
		if !ok {
			err = fmt.Errorf("tag struct does not have field %s", datas.TagCommitRefField)
			return types.Struct{}, err
		}

		commitSt, err := commitRef.(types.Ref).TargetValue(ctx, vrw)
		if err != nil {
			return types.Struct{}, err
		}

		return commitSt.(types.Struct), nil
	}

	err = fmt.Errorf("dataset head is neither commit nor tag")
	return types.Struct{}, err
}

func getCommitStForHash(ctx context.Context, vr types.ValueReader, c string) (types.Struct, error) {
	unprefixed := strings.TrimPrefix(c, "#")
	hash, ok := hash.MaybeParse(unprefixed)
	if !ok {
		return types.Struct{}, errors.New("invalid hash: " + c)
	}

	val, err := vr.ReadValue(ctx, hash)
	if err != nil {
		return types.Struct{}, err
	}
	if val == nil {
		return types.Struct{}, ErrHashNotFound
	}

	valSt, ok := val.(types.Struct)
	if !ok || valSt.Name() != CommitStructName {
		return types.Struct{}, ErrFoundHashNotACommit
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
		commitSt, err = getCommitStForHash(ctx, ddb.vrw, cs.baseSpec)
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
			commitSt, err = getCommitStForRefStr(ctx, ddb.db, ddb.vrw, candidate)
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
		commitSt, err = getCommitStForRefStr(ctx, ddb.db, ddb.vrw, cwb.String())
	default:
		panic("unrecognized commit spec csType: " + cs.csType)
	}

	if err != nil {
		return nil, err
	}

	commitSt, err = getAncestor(ctx, ddb.vrw, commitSt, cs.aSpec)

	if err != nil {
		return nil, err
	}

	return NewCommit(ddb.vrw, commitSt), nil
}

// ResolveCommitRef takes a DoltRef and returns a Commit, or an error if the commit cannot be found. The ref given must
// point to a Commit.
func (ddb *DoltDB) ResolveCommitRef(ctx context.Context, ref ref.DoltRef) (*Commit, error) {
	commitSt, err := getCommitStForRefStr(ctx, ddb.db, ddb.vrw, ref.String())
	if err != nil {
		return nil, err
	}
	return NewCommit(ddb.vrw, commitSt), nil
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

	return NewTag(ctx, tagRef.GetPath(), ddb.vrw, tagSt)
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

	return NewWorkingSet(ctx, workingSetRef.GetPath(), ddb.vrw, wsSt)
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

	return ddb.vrw.WriteValue(ctx, rv.valueSt)
}

// ReadRootValue reads the RootValue associated with the hash given and returns it. Returns an error if the value cannot
// be read, or if the hash given doesn't represent a dolt RootValue.
func (ddb *DoltDB) ReadRootValue(ctx context.Context, h hash.Hash) (*RootValue, error) {
	val, err := ddb.vrw.ReadValue(ctx, h)

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

	return newRootValue(ddb.vrw, rootSt)
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

	rf, err := types.NewRef(commit.commitSt, ddb.vrw.Format())

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

	stRef, err := types.NewRef(cm.commitSt, ddb.vrw.Format())

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
	val, err := ddb.vrw.ReadValue(ctx, valHash)

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

	l, err := types.NewList(ctx, ddb.vrw)

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
		rf, err := types.NewRef(cm.commitSt, ddb.vrw.Format())

		if err != nil {
			return nil, err
		}

		parentEditor = parentEditor.Append(rf)
	}

	parents, err := parentEditor.List(ctx)

	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.vrw.Format())

	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: st}
	ds, err = ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return nil, err
	}

	ds, err = ddb.db.Commit(ctx, ds, val, commitOpts)

	if err != nil {
		return nil, err
	}

	commitSt, ok := ds.MaybeHead()
	if !ok {
		return nil, errors.New("Commit has no head but commit succeeded. This is a bug.")
	}

	return NewCommit(ddb.vrw, commitSt), nil
}

// dangling commits are unreferenced by any branch or ref. They are created in the course of programmatic updates
// such as rebase. You must create a ref to a dangling commit for it to be reachable
func (ddb *DoltDB) CommitDanglingWithParentCommits(ctx context.Context, valHash hash.Hash, parentCommits []*Commit, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	val, err := ddb.vrw.ReadValue(ctx, valHash)
	if err != nil {
		return nil, err
	}
	if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	l, err := types.NewList(ctx, ddb.vrw)
	if err != nil {
		return nil, err
	}

	parentEditor := l.Edit()

	for _, cm := range parentCommits {
		rf, err := types.NewRef(cm.commitSt, ddb.vrw.Format())
		if err != nil {
			return nil, err
		}

		parentEditor = parentEditor.Append(rf)
	}

	parents, err := parentEditor.List(ctx)
	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.vrw.Format())
	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: st}
	commitSt, err = datas.NewCommitForValue(ctx, ddb.vrw, val, commitOpts)
	if err != nil {
		return nil, err
	}

	_, err = ddb.vrw.WriteValue(ctx, commitSt)
	if err != nil {
		return nil, err
	}

	return NewCommit(ddb.vrw, commitSt), nil
}

// ValueReadWriter returns the underlying noms database as a types.ValueReadWriter.
func (ddb *DoltDB) ValueReadWriter() types.ValueReadWriter {
	return ddb.vrw
}

func (ddb *DoltDB) Format() *types.NomsBinFormat {
	return ddb.vrw.Format()
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

type BranchWithHash struct {
	Ref  ref.DoltRef
	Hash hash.Hash
}

func (ddb *DoltDB) GetBranchesWithHashes(ctx context.Context) ([]BranchWithHash, error) {
	var refs []BranchWithHash
	err := ddb.VisitRefsOfType(ctx, branchRefFilter, func(r ref.DoltRef, addr hash.Hash) error {
		refs = append(refs, BranchWithHash{r, addr})
		return nil
	})
	return refs, err
}

var tagsRefFilter = map[ref.RefType]struct{}{ref.TagRefType: {}}

// GetTags returns a list of all tags in the database.
func (ddb *DoltDB) GetTags(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, tagsRefFilter)
}

type TagWithHash struct {
	Ref  ref.DoltRef
	Hash hash.Hash
}

// GetTagsWithHashes returns a list of objects containing TagRefs with their associated Commit's hash
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
			refs = append(refs, TagWithHash{r, h})
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

	return dss.IterAll(ctx, func(key string, addr hash.Hash) error {
		keyStr := key

		var dref ref.DoltRef
		if ref.IsRef(keyStr) {
			dref, err = ref.Parse(keyStr)
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

func (ddb *DoltDB) GetRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}) ([]ref.DoltRef, error) {
	var refs []ref.DoltRef
	err := ddb.VisitRefsOfType(ctx, refTypeFilter, func(r ref.DoltRef, _ hash.Hash) error {
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

	rf, err := types.NewRef(commit.commitSt, ddb.vrw.Format())
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
	if err != nil {
		return err
	}

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
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, currWsHash, TodoWorkingSetMeta())
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

	return ddb.UpdateWorkingSet(ctx, toWSRef, ws, currWsHash, TodoWorkingSetMeta())
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

	st, err := meta.toNomsStruct(ddb.vrw.Format())

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

	metaSt, err := meta.toNomsStruct(types.Format_Default)
	if err != nil {
		return err
	}

	_, err = ddb.db.UpdateWorkingSet(ctx, ds, datas.WorkingSetSpec{
		Meta:        datas.WorkingSetMeta{Meta: metaSt},
		WorkingRoot: workingRootRef,
		StagedRoot:  stagedRef,
		MergeState:  mergeStateRef,
	}, prevHash)

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
	meta *WorkingSetMeta,
) (*Commit, error) {
	wsDs, err := ddb.db.GetDataset(ctx, workingSetRef.String())
	if err != nil {
		return nil, err
	}

	headDs, err := ddb.db.GetDataset(ctx, headRef.String())
	if err != nil {
		return nil, err
	}

	workingRootRef, stagedRef, mergeStateRef, err := workingSet.writeValues(ctx, ddb)
	if err != nil {
		return nil, err
	}

	var metaSt types.Struct
	metaSt, err = meta.toNomsStruct(ddb.vrw.Format())
	if err != nil {
		return nil, err
	}

	commitDataset, _, err := ddb.db.CommitWithWorkingSet(ctx, headDs, wsDs, commit.Roots.Staged.valueSt, datas.WorkingSetSpec{
		Meta:        datas.WorkingSetMeta{Meta: metaSt},
		WorkingRoot: workingRootRef,
		StagedRoot:  stagedRef,
		MergeState:  mergeStateRef,
	}, prevHash, commit.CommitOptions)

	if err != nil {
		return nil, err
	}

	commitSt, ok := commitDataset.MaybeHead()
	if !ok {
		return nil, errors.New("Commit has no head but commit succeeded. This is a bug.")
	}

	return NewCommit(ddb.vrw, commitSt), nil
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

// Rebase rebases the underlying db from disk, re-loading the manifest. Useful when another process might have made
// changes to the database we need to read.
func (ddb *DoltDB) Rebase(ctx context.Context) error {
	return datas.ChunkStoreFromDatabase(ddb.db).Rebase(ctx)
}

// GC performs garbage collection on this ddb. Values passed in |uncommitedVals| will be temporarily saved during gc.
func (ddb *DoltDB) GC(ctx context.Context, uncommitedVals ...hash.Hash) error {
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
	newGen := hash.NewHashSet(uncommitedVals...)
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

	return collector.GC(ctx, oldGen, newGen)
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

// PullChunks initiates a pull into this database from the source database
// given, pulling all chunks reachable from the given targetHash. Pull progress
// is communicated over the provided channel.
func (ddb *DoltDB) PullChunks(ctx context.Context, tempDir string, srcDB *DoltDB, targetHash hash.Hash, progChan chan pull.PullProgress, pullerEventCh chan pull.PullerEvent) error {
	srcCS := datas.ChunkStoreFromDatabase(srcDB.db)
	destCS := datas.ChunkStoreFromDatabase(ddb.db)
	wrf, err := types.WalkRefsForChunkStore(srcCS)
	if err != nil {
		return err
	}

	if datas.CanUsePuller(srcDB.db) && datas.CanUsePuller(ddb.db) {
		puller, err := pull.NewPuller(ctx, tempDir, defaultChunksPerTF, srcCS, destCS, wrf, targetHash, pullerEventCh)
		if err == pull.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return pull.Pull(ctx, srcCS, destCS, wrf, targetHash, progChan)
	}
}

func (ddb *DoltDB) Clone(ctx context.Context, destDB *DoltDB, eventCh chan<- pull.TableFileEvent) error {
	return pull.Clone(ctx, datas.ChunkStoreFromDatabase(ddb.db), datas.ChunkStoreFromDatabase(destDB.db), eventCh)
}

func (ddb *DoltDB) SetCommitHooks(ctx context.Context, postHooks []CommitHook) *DoltDB {
	ddb.db = ddb.db.SetCommitHooks(ctx, postHooks)
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
	ddb.db.ExecuteCommitHooks(ctx, ds)
	return nil
}
