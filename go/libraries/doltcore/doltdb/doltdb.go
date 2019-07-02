package doltdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types/edits"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func init() {
	types.CreateEditAccForMapEdits = func(f *types.Format) types.EditAccumulator {
		return edits.NewAsyncSortedEdits(f, 16*1024, 4, 2)
	}
}

const (
	creationBranch   = "create"
	MasterBranch     = "master"
	CommitStructName = "Commit"
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
func LoadDoltDB(ctx context.Context, urlStr string) (*DoltDB, error) {
	return LoadDoltDBWithParams(ctx, urlStr, nil)
}

func LoadDoltDBWithParams(ctx context.Context, urlStr string, params map[string]string) (*DoltDB, error) {
	if urlStr == LocalDirDoltDB {
		exists, isDir := filesys.LocalFS.Exists(dbfactory.DoltDataDir)

		if !exists {
			return nil, errors.New("missing dolt data directory")
		} else if !isDir {
			return nil, errors.New("file exists where the dolt data directory should be")
		}
	}

	db, err := dbfactory.CreateDB(ctx, urlStr, params)

	if err != nil {
		return nil, err
	}

	return &DoltDB{db}, nil
}

// WriteEmptyRepo will create initialize the given db with a master branch which points to a commit which has valid
// metadata for the creation commit, and an empty RootValue.
func (ddb *DoltDB) WriteEmptyRepo(ctx context.Context, name, email string) error {
	if ddb.db.GetDataset(ctx, creationBranch).HasHead() {
		return errors.New("database already exists")
	}

	name = strings.TrimSpace(name)
	email = strings.TrimSpace(email)

	if name == "" || email == "" {
		panic("Passed bad name or email.  Both should be valid")
	}

	err := pantoerr.PanicToError("Failed to write empty repo", func() error {
		rv := emptyRootValue(ctx, ddb.db)
		_, err := ddb.WriteRootValue(ctx, rv)

		cm, _ := NewCommitMeta(name, email, "Data repository created.")

		commitOpts := datas.CommitOptions{Parents: types.Set{}, Meta: cm.toNomsStruct(), Policy: nil}

		dref := ref.NewInternalRef(creationBranch)
		firstCommit, err := ddb.db.Commit(ctx, ddb.db.GetDataset(ctx, dref.String()), rv.valueSt, commitOpts)

		if err != nil {
			return err
		}

		dref = ref.NewBranchRef(MasterBranch)
		_, err = ddb.db.SetHead(ctx, ddb.db.GetDataset(ctx, dref.String()), firstCommit.HeadRef())

		return err
	})

	return err
}

func getCommitStForRef(ctx context.Context, db datas.Database, dref ref.DoltRef) (types.Struct, error) {
	ds := db.GetDataset(ctx, dref.String())

	if ds.HasHead() {
		return ds.Head(), nil
	}

	return types.EmptyStruct(types.Format_7_18), ErrBranchNotFound
}

func getCommitStForHash(ctx context.Context, db datas.Database, c string) (types.Struct, error) {
	prefixed := c

	if !strings.HasPrefix(c, "#") {
		prefixed = "#" + c
	}

	ap, err := spec.NewAbsolutePath(types.Format_7_18, prefixed)

	if err != nil {
		return types.EmptyStruct(types.Format_7_18), err
	}

	val := ap.Resolve(ctx, db)

	if val == nil {
		return types.EmptyStruct(types.Format_7_18), ErrHashNotFound
	}

	valSt, ok := val.(types.Struct)

	if !ok || valSt.Name() != CommitStructName {
		return types.EmptyStruct(types.Format_7_18), ErrFoundHashNotACommit
	}

	return valSt, nil
}

func walkAncestorSpec(ctx context.Context, db datas.Database, commitSt types.Struct, aSpec *AncestorSpec) (types.Struct, error) {
	if aSpec == nil || len(aSpec.Instructions) == 0 {
		return commitSt, nil
	}

	instructions := aSpec.Instructions
	for _, inst := range instructions {
		cm := Commit{db, commitSt}

		if inst < cm.NumParents() {
			commitStPtr := cm.getParent(ctx, inst)

			if commitStPtr == nil {
				return types.EmptyStruct(types.Format_7_18), ErrInvalidAnscestorSpec
			}
			commitSt = *commitStPtr
		} else {
			return types.EmptyStruct(types.Format_7_18), ErrInvalidAnscestorSpec
		}
	}

	return commitSt, nil
}

// Resolve takes a CommitSpec and returns a Commit, or an error if the commit cannot be found.
func (ddb *DoltDB) Resolve(ctx context.Context, cs *CommitSpec) (*Commit, error) {
	if cs == nil {
		panic("nil commit spec")
	}

	var commitSt types.Struct
	err := pantoerr.PanicToError("unable to resolve commit "+cs.CommitStringer.String(), func() error {
		var err error
		if cs.CSType == HashCommitSpec {
			commitSt, err = getCommitStForHash(ctx, ddb.db, cs.CommitStringer.String())
		} else if cs.CSType == RefCommitSpec {
			commitSt, err = getCommitStForRef(ctx, ddb.db, cs.CommitStringer.(ref.DoltRef))
		}

		if err != nil {
			return err
		}

		commitSt, err = walkAncestorSpec(ctx, ddb.db, commitSt, cs.ASpec)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
}

// WriteRootValue will write a doltdb.RootValue instance to the database.  This value will not be associated with a commit
// and can be committed by hash at a later time.  Returns the hash of the value written.
func (ddb *DoltDB) WriteRootValue(ctx context.Context, rv *RootValue) (hash.Hash, error) {
	var valHash hash.Hash
	err := pantoerr.PanicToErrorNil("failed to write value", func() {
		ref := ddb.db.WriteValue(ctx, rv.valueSt)
		ddb.db.Flush(ctx)

		valHash = ref.TargetHash()
	})

	return valHash, err
}

// ReadRootValue reads the RootValue associated with the hash given and returns it. Returns an error if the value cannot
// be read, or if the hash given doesn't represent a dolt RootValue.
func (ddb *DoltDB) ReadRootValue(ctx context.Context, h hash.Hash) (*RootValue, error) {
	var val types.Value
	err := pantoerr.PanicToErrorNil("unable to read root value", func() {
		val = ddb.db.ReadValue(ctx, h)
	})

	if err != nil {
		return nil, err
	}

	if val != nil {
		if rootSt, ok := val.(types.Struct); ok {
			return &RootValue{ddb.db, rootSt}, nil
		}
	}

	return nil, errors.New("there is no dolt root value at that hash")
}

// Commit will update a branch's head value to be that of a previously committed root value hash
func (ddb *DoltDB) Commit(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, cm *CommitMeta) (*Commit, error) {
	if dref.GetType() != ref.BranchRefType {
		panic("can't commit to ref that isn't branch atm.  will probably remove this.")
	}

	return ddb.CommitWithParents(ctx, valHash, dref, nil, cm)
}

// FastForward fast-forwards the branch given to the commit given.
func (ddb *DoltDB) FastForward(ctx context.Context, branch ref.DoltRef, commit *Commit) error {
	ds := ddb.db.GetDataset(ctx, branch.String())
	_, err := ddb.db.FastForward(ctx, ds, types.NewRef(commit.commitSt, types.Format_7_18))

	return err
}

// CanFastForward returns whether the given branch can be fast-forwarded to the commit given.
func (ddb *DoltDB) CanFastForward(ctx context.Context, branch ref.DoltRef, new *Commit) (bool, error) {
	currentSpec, _ := NewCommitSpec("HEAD", branch.String())
	current, err := ddb.Resolve(ctx, currentSpec)

	if err != nil {
		if err == ErrBranchNotFound {
			return true, nil
		}

		return false, err
	}

	return current.CanFastForwardTo(ctx, new)
}

// CommitWithParents commits the value hash given to the branch given, using the list of parent hashes given. Returns an
// error if the value or any parents can't be resolved, or if anything goes wrong accessing the underlying storage.
func (ddb *DoltDB) CommitWithParents(ctx context.Context, valHash hash.Hash, dref ref.DoltRef, parentCmSpecs []*CommitSpec, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	err := pantoerr.PanicToError("error committing value "+valHash.String(), func() error {
		val := ddb.db.ReadValue(ctx, valHash)

		if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
			return errors.New("can't commit a value that is not a valid root value")
		}

		ds := ddb.db.GetDataset(ctx, dref.String())
		parentEditor := types.NewSet(ctx, types.Format_7_18, ddb.db).Edit()

		if ds.HasHead() {
			parentEditor.Insert(ds.HeadRef())
		}

		for _, parentCmSpec := range parentCmSpecs {
			cs, err := ddb.Resolve(ctx, parentCmSpec)

			if err != nil {
				return err
			}

			parentEditor.Insert(types.NewRef(cs.commitSt, types.Format_7_18))
		}

		parents := parentEditor.Set(ctx)
		commitOpts := datas.CommitOptions{Parents: parents, Meta: cm.toNomsStruct(), Policy: nil}
		ds, err := ddb.db.Commit(ctx, ddb.db.GetDataset(ctx, dref.String()), val, commitOpts)

		if ds.HasHead() {
			commitSt = ds.Head()
		} else if err == nil {
			return errors.New("commit has no head but commit succeeded (How?!?!?)")
		}

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

func writeValAndGetRef(ctx context.Context, vrw types.ValueReadWriter, val types.Value) types.Ref {
	valRef := types.NewRef(val, types.Format_7_18)

	targetVal := valRef.TargetValue(ctx, vrw)

	if targetVal == nil {
		vrw.WriteValue(ctx, val)
	}

	return valRef
}

// ResolveParent returns the n-th ancestor of a given commit (direct parent is index 0). error return value will be
// non-nil in the case that the commit cannot be resolved, there aren't as many ancestors as requested, or the
// underlying storage cannot be accessed.
func (ddb *DoltDB) ResolveParent(ctx context.Context, commit *Commit, parentIdx int) (*Commit, error) {
	var parentCommitSt types.Struct
	errMsg := fmt.Sprintf("failed to resolve parent of %s", commit.HashOf().String())
	err := pantoerr.PanicToErrorNil(errMsg, func() {
		parentSet := commit.getParents()
		itr := parentSet.IteratorAt(ctx, uint64(parentIdx))
		parentCommRef := itr.Next(ctx)

		parentVal := parentCommRef.(types.Ref).TargetValue(ctx, ddb.ValueReadWriter())
		parentCommitSt = parentVal.(types.Struct)
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.ValueReadWriter(), parentCommitSt}, nil
}

// HasBranch returns whether the branch given exists in this database.
func (ddb *DoltDB) HasRef(ctx context.Context, doltRef ref.DoltRef) bool {
	return ddb.db.Datasets(ctx).Has(ctx, types.String(doltRef.String()))
}

var branchRefFilter = map[ref.RefType]struct{}{ref.BranchRefType: {}}

// GetBranches returns a list of all branches in the database.
func (ddb *DoltDB) GetBranches(ctx context.Context) []ref.DoltRef {
	return ddb.GetRefsOfType(ctx, branchRefFilter)
}

func (ddb *DoltDB) GetRefs(ctx context.Context) []ref.DoltRef {
	return ddb.GetRefsOfType(ctx, ref.RefTypes)
}

func (ddb *DoltDB) GetRefsOfType(ctx context.Context, refTypeFilter map[ref.RefType]struct{}) []ref.DoltRef {
	var branches []ref.DoltRef
	ddb.db.Datasets(ctx).IterAll(ctx, func(key, _ types.Value) {
		keyStr := string(key.(types.String))

		var dref ref.DoltRef
		if ref.IsRef(keyStr) {
			dref, _ = ref.Parse(keyStr)

			if _, ok := refTypeFilter[dref.GetType()]; ok {
				branches = append(branches, dref)
			}
		}
	})

	return branches
}

// NewBranchAtCommit creates a new branch with HEAD at the commit given. Branch names must pass IsValidUserBranchName.
func (ddb *DoltDB) NewBranchAtCommit(ctx context.Context, dref ref.DoltRef, commit *Commit) error {
	if !IsValidBranchRef(dref) {
		panic(fmt.Sprintf("invalid branch name %s, use IsValidUserBranchName check", dref.String()))
	}

	ds := ddb.db.GetDataset(ctx, dref.String())
	_, err := ddb.db.SetHead(ctx, ds, types.NewRef(commit.commitSt, types.Format_7_18))
	return err
}

// DeleteBranch deletes the branch given, returning an error if it doesn't exist.
func (ddb *DoltDB) DeleteBranch(ctx context.Context, dref ref.DoltRef) error {
	ds := ddb.db.GetDataset(ctx, dref.String())

	if !ds.HasHead() {
		return ErrBranchNotFound
	}

	_, err := ddb.db.Delete(ctx, ds)
	return err
}

// PushChunks initiates a push into a database from the source database given, at the commit given. Pull progress is
// communicated over the provided channel.
func (ddb *DoltDB) PushChunks(ctx context.Context, srcDB *DoltDB, cm *Commit, progChan chan datas.PullProgress) error {
	datas.Pull(ctx, srcDB.db, ddb.db, types.NewRef(cm.commitSt, types.Format_7_18), progChan)
	return nil
}

// PullChunks initiates a pull into a database from the source database given, at the commit given. Progress is
// communicated over the provided channel.
func (ddb *DoltDB) PullChunks(ctx context.Context, srcDB *DoltDB, cm *Commit, progChan chan datas.PullProgress) error {
	datas.PullWithoutBatching(ctx, srcDB.db, ddb.db, types.NewRef(cm.commitSt, types.Format_7_18), progChan)
	return nil
}
