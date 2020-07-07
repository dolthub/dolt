// Copyright 2019 Liquidata, Inc.
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
	"strings"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/spec"
	"github.com/liquidata-inc/dolt/go/store/types/edits"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
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

// DoltDB wraps access to the underlying noms database and hides some of the details of the underlying storage.
// Additionally the noms codebase uses panics in a way that is non idiomatic and I've opted to recover and return
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
func LoadDoltDB(ctx context.Context, nbf *types.NomsBinFormat, urlStr string) (*DoltDB, error) {
	return LoadDoltDBWithParams(ctx, nbf, urlStr, nil)
}

func LoadDoltDBWithParams(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, params map[string]string) (*DoltDB, error) {
	if urlStr == LocalDirDoltDB {
		exists, isDir := filesys.LocalFS.Exists(dbfactory.DoltDataDir)

		if !exists {
			return nil, errors.New("missing dolt data directory")
		} else if !isDir {
			return nil, errors.New("file exists where the dolt data directory should be")
		}
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

	rv, err := emptyRootValue(ctx, ddb.db)

	if err != nil {
		return err
	}

	_, err = ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	cm, _ := NewCommitMetaWithUserTS(name, email, "Initialize data repository", t)

	parentSet, err := types.NewSet(ctx, ddb.db)

	if err != nil {
		return err
	}

	meta, err := cm.toNomsStruct(ddb.db.Format())

	if err != nil {
		return err
	}

	commitOpts := datas.CommitOptions{Parents: parentSet, Meta: meta, Policy: nil}

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

func getCommitStForDatasetName(ctx context.Context, db datas.Database, datasetName string) (types.Struct, error) {
	ds, err := db.GetDataset(ctx, datasetName)

	if err != nil {
		return types.EmptyStruct(db.Format()), err
	}

	dsHead, hasHead := ds.MaybeHead()
	if hasHead {
		return dsHead, nil
	}

	return types.EmptyStruct(db.Format()), ErrBranchNotFound
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
		cm := Commit{vrw, commitSt}

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

// Resolve takes a CommitSpec and returns a Commit, or an error if the commit cannot be found.
// If the CommitSpec is a HEAD, Resolve also needs the DoltRef of the current working branch.
func (ddb *DoltDB) Resolve(ctx context.Context, cs *CommitSpec, cwb ref.DoltRef) (*Commit, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	var commitSt types.Struct
	var err error
	switch cs.CSType {
	case HashCommitSpec:
		commitSt, err = getCommitStForHash(ctx, ddb.db, cs.BaseRef)
	case RefCommitSpec:
		// For a ref in a CommitSpec, we have the following behavior.
		// If it starts with `refs/`, we look for an exact match before
		// we try any suffix matches. After that, we try a match on the
		// user supplied input, with the following three prefixes, in
		// order: `refs/`, `refs/heads/`, `refs/remotes/`.
		candidates := []string{
			"refs/" + cs.BaseRef,
			"refs/heads/" + cs.BaseRef,
			"refs/remotes/" + cs.BaseRef,
		}
		if strings.HasPrefix(cs.BaseRef, "refs/") {
			candidates = []string {
				cs.BaseRef,
				"refs/" + cs.BaseRef,
				"refs/heads/" + cs.BaseRef,
				"refs/remotes/" + cs.BaseRef,
			}
		}
		for _, candidate := range candidates {
			commitSt, err = getCommitStForDatasetName(ctx, ddb.db, candidate)
			if err == nil {
				break
			}
			if err != ErrBranchNotFound {
				return nil, err
			}
		}
	case headCommitSpec:
		commitSt, err = getCommitStForDatasetName(ctx, ddb.db, cwb.String())
	default:
		panic("unrecognized commit spec CSType: " + cs.CSType)
	}

	if err != nil {
		return nil, err
	}

	commitSt, err = getAncestor(ctx, ddb.db, commitSt, cs.ASpec)

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
}

// TODO: convenience method to resolve the head commit of a branch.

// WriteRootValue will write a doltdb.RootValue instance to the database.  This value will not be associated with a commit
// and can be committed by hash at a later time.  Returns the hash of the value written.
func (ddb *DoltDB) WriteRootValue(ctx context.Context, rv *RootValue) (hash.Hash, error) {
	valRef, err := ddb.db.WriteValue(ctx, rv.valueSt)

	if err != nil {
		return hash.Hash{}, err
	}

	err = ddb.db.Flush(ctx)

	if err != nil {
		return hash.Hash{}, err
	}

	valHash := valRef.TargetHash()

	return valHash, err
}

// ReadRootValue reads the RootValue associated with the hash given and returns it. Returns an error if the value cannot
// be read, or if the hash given doesn't represent a dolt RootValue.
func (ddb *DoltDB) ReadRootValue(ctx context.Context, h hash.Hash) (*RootValue, error) {
	val, err := ddb.db.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	if val != nil {
		if rootSt, ok := val.(types.Struct); ok && rootSt.Name() == ddbRootStructName {
			return &RootValue{ddb.db, rootSt, nil}, nil
		}
	}

	return nil, errors.New("there is no dolt root value at that hash")
}

// Commit will update a branch's head value to be that of a previously committed root value hash
func (ddb *DoltDB) Commit(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, cm *CommitMeta) (*Commit, error) {
	if dref.GetType() != ref.BranchRefType {
		panic("can't commit to ref that isn't branch atm.  will probably remove this.")
	}

	// TODO: this nil seems wrong
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
	currentSpec, _ := NewCommitSpec(branch.String())
	current, err := ddb.Resolve(ctx, currentSpec, nil)

	if err != nil {
		if err == ErrBranchNotFound {
			return true, nil
		}

		return false, err
	}

	return current.CanFastForwardTo(ctx, new)
}

// SetHead sets the given ref to point at the given commit. It is used in the course of 'force' updates.
func (ddb *DoltDB) SetHead(ctx context.Context, ref ref.DoltRef, cm *Commit) error {
	ds, err := ddb.db.GetDataset(ctx, ref.String())

	if err != nil {
		return err
	}

	r, err := types.NewRef(cm.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, r)
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

func (ddb *DoltDB) WriteDanglingCommit(ctx context.Context, valHash hash.Hash, parentCommits []*Commit, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	val, err := ddb.db.ReadValue(ctx, valHash)

	if err != nil {
		return nil, err
	}

	if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
		return nil, errors.New("can't commit a value that is not a valid root value")
	}

	s, err := types.NewSet(ctx, ddb.db)

	if err != nil {
		return nil, err
	}

	parentEditor := s.Edit()

	for _, cm := range parentCommits {
		rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

		if err != nil {
			return nil, err
		}

		_, err = parentEditor.Insert(rf)

		if err != nil {
			return nil, err
		}
	}

	parents, err := parentEditor.Set(ctx)

	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.db.Format())

	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{Parents: parents, Meta: st, Policy: nil}
	commitSt, err = ddb.db.CommitDangling(ctx, val, commitOpts)

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
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

	s, err := types.NewSet(ctx, ddb.db)

	if err != nil {
		return nil, err
	}

	parentEditor := s.Edit()

	headRef, hasHead, err := ds.MaybeHeadRef()

	if err != nil {
		return nil, err
	}

	if hasHead {
		_, err := parentEditor.Insert(headRef)

		if err != nil {
			return nil, err
		}
	}

	for _, cm := range parentCommits {
		rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

		if err != nil {
			return nil, err
		}

		_, err = parentEditor.Insert(rf)

		if err != nil {
			return nil, err
		}
	}

	parents, err := parentEditor.Set(ctx)

	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.db.Format())

	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{Parents: parents, Meta: st, Policy: nil}
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

	return &Commit{ddb.db, commitSt}, nil
}

// dangling commits are unreferenced by any branch or ref. They are created in the course of programmatic updates
// such as rebase. You must create a ref to a dangling commit for it to be reachable
func (ddb *DoltDB) CommitDanglingWithParentCommits(ctx context.Context, valHash hash.Hash, parentCommits []*Commit, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	err := pantoerr.PanicToError("error committing value "+valHash.String(), func() error {
		val, err := ddb.db.ReadValue(ctx, valHash)

		if err != nil {
			return err
		}

		if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
			return errors.New("can't commit a value that is not a valid root value")
		}

		s, err := types.NewSet(ctx, ddb.db)

		if err != nil {
			return err
		}

		parentEditor := s.Edit()

		for _, cm := range parentCommits {
			rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

			if err != nil {
				return err
			}

			_, err = parentEditor.Insert(rf)

			if err != nil {
				return err
			}
		}

		// even orphans have parents
		parents, err := parentEditor.Set(ctx)

		if err != nil {
			return err
		}

		st, err := cm.toNomsStruct(ddb.db.Format())

		if err != nil {
			return err
		}

		commitOpts := datas.CommitOptions{Parents: parents, Meta: st, Policy: nil}

		commitSt, err = ddb.db.CommitDangling(ctx, val, commitOpts)

		return err
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
}

// ValueReadWriter returns the underlying noms database as a types.ValueReadWriter.
func (ddb *DoltDB) ValueReadWriter() types.ValueReadWriter {
	return ddb.db
}

func (ddb *DoltDB) Format() *types.NomsBinFormat {
	return ddb.db.Format()
}

func writeValAndGetRef(ctx context.Context, vrw types.ValueReadWriter, val types.Value) (types.Ref, error) {
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
	var parentCommitSt types.Struct
	parentSet, err := commit.getParents()

	if err != nil {
		return nil, err
	}

	itr, err := parentSet.IteratorAt(ctx, uint64(parentIdx))

	if err != nil {
		return nil, err
	}

	parentCommRef, err := itr.Next(ctx)

	if err != nil {
		return nil, err
	}

	parentVal, err := parentCommRef.(types.Ref).TargetValue(ctx, ddb.ValueReadWriter())

	if err != nil {
		return nil, err
	}

	parentCommitSt = parentVal.(types.Struct)

	return &Commit{ddb.ValueReadWriter(), parentCommitSt}, nil
}

func (ddb *DoltDB) ResolveAllParents(ctx context.Context, commit *Commit) ([]*Commit, error) {
	var parentCommitSt types.Struct
	parentSet, err := commit.getParents()

	if err != nil {
		return nil, err
	}

	itr, err := parentSet.IteratorAt(ctx, 0)

	if err != nil {
		return nil, err
	}

	var allParents []*Commit
	for i := 0; i < int(parentSet.Len()); i++ {
		parentCommRef, err := itr.Next(ctx)
		if err != nil {
			return nil, err
		}

		parentVal, err := parentCommRef.(types.Ref).TargetValue(ctx, ddb.ValueReadWriter())

		if err != nil {
			return nil, err
		}

		parentCommitSt = parentVal.(types.Struct)

		allParents = append(allParents, &Commit{ddb.ValueReadWriter(), parentCommitSt})
	}
	return allParents, nil
}

// HasBranch returns whether the branch given exists in this database.
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

func (ddb *DoltDB) GetRefs(ctx context.Context) ([]ref.DoltRef, error) {
	return ddb.GetRefsOfType(ctx, ref.RefTypes)
}

func (ddb *DoltDB) GetRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}) ([]ref.DoltRef, error) {
	var branches []ref.DoltRef
	dss, err := ddb.db.Datasets(ctx)

	if err != nil {
		return nil, err
	}

	err = dss.IterAll(ctx, func(key, _ types.Value) error {
		keyStr := string(key.(types.String))

		var dref ref.DoltRef
		if ref.IsRef(keyStr) {
			dref, _ = ref.Parse(keyStr)

			if _, ok := refTypeFilter[dref.GetType()]; ok {
				branches = append(branches, dref)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return branches, nil
}

// NewBranchAtCommit creates a new branch with HEAD at the commit given. Branch names must pass IsValidUserBranchName.
func (ddb *DoltDB) NewBranchAtCommit(ctx context.Context, dref ref.DoltRef, commit *Commit) error {
	if !IsValidBranchRef(dref) {
		panic(fmt.Sprintf("invalid branch name %s, use IsValidUserBranchName check", dref.String()))
	}

	ds, err := ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return err
	}

	rf, err := types.NewRef(commit.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

	_, err = ddb.db.SetHead(ctx, ds, rf)

	return err
}

// DeleteBranch deletes the branch given, returning an error if it doesn't exist.
func (ddb *DoltDB) DeleteBranch(ctx context.Context, dref ref.DoltRef) error {
	ds, err := ddb.db.GetDataset(ctx, dref.String())

	if err != nil {
		return err
	}

	if !ds.HasHead() {
		return ErrBranchNotFound
	}

	_, err = ddb.db.Delete(ctx, ds)
	return err
}

// PushChunks initiates a push into a database from the source database given, at the commit given. Pull progress is
// communicated over the provided channel.
func (ddb *DoltDB) PushChunks(ctx context.Context, tempDir string, srcDB *DoltDB, cm *Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

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

// PullChunks initiates a pull into a database from the source database given, at the commit given. Progress is
// communicated over the provided channel.
func (ddb *DoltDB) PullChunks(ctx context.Context, tempDir string, srcDB *DoltDB, cm *Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	rf, err := types.NewRef(cm.commitSt, ddb.db.Format())

	if err != nil {
		return err
	}

	if datas.CanUsePuller(srcDB.db) && datas.CanUsePuller(ddb.db) {
		puller, err := datas.NewPuller(ctx, tempDir, 256*1024, srcDB.db, ddb.db, rf.TargetHash(), pullerEventCh)

		if err == datas.ErrDBUpToDate {
			return nil
		} else if err != nil {
			return err
		}

		return puller.Pull(ctx)
	} else {
		return datas.PullWithoutBatching(ctx, srcDB.db, ddb.db, rf, progChan)
	}
}

func (ddb *DoltDB) Clone(ctx context.Context, destDB *DoltDB, eventCh chan<- datas.TableFileEvent) error {
	return datas.Clone(ctx, ddb.db, destDB.db, eventCh)
}
