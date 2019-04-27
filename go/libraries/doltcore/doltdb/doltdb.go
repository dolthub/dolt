package doltdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/chunks"
	"path/filepath"
	"strings"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

const (
	creationBranch   = "__create__"
	MasterBranch     = "master"
	CommitStructName = "Commit"
)

// Location represents a location where a DoltDB database lives.
type Location string

const (
	// InMemDoltDB stores the DoltDB db in memory and is primarily used for testing
	InMemDoltDB = Location("mem")

	DoltDir = ".dolt"
	DataDir = "noms"
)

var DoltDataDir = filepath.Join(DoltDir, DataDir)

// LocalDirDoltDB stores the db in the current directory
var LocalDirDoltDB = Location("nbs:" + DoltDataDir)

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
func LoadDoltDB(loc Location) *DoltDB {
	if loc == LocalDirDoltDB {
		exists, isDir := filesys.LocalFS.Exists(DoltDataDir)

		if !exists {
			return nil
		} else if !isDir {
			panic("A file exists where the dolt data directory should be.")
		}
	}

	dbSpec, _ := spec.ForDatabase(string(loc))

	// There is the possibility of this panicking, but have decided specifically not to recover (as is normally done in
	// this codebase. For failure to occur getting a database for the current directory, or an in memory database
	// something would have to be drastically wrong.
	db := dbSpec.GetDatabase(context.TODO())
	return &DoltDB{db}
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
		firstCommit, err := ddb.db.Commit(ctx, ddb.db.GetDataset(ctx, creationBranch), rv.valueSt, commitOpts)

		if err != nil {
			return err
		}

		_, err = ddb.db.SetHead(ctx, ddb.db.GetDataset(ctx, MasterBranch), firstCommit.HeadRef())

		return err
	})

	return err
}

func getCommitStForBranch(ctx context.Context, db datas.Database, b string) (types.Struct, error) {
	ds := db.GetDataset(ctx, b)

	if ds.HasHead() {
		return ds.Head(), nil
	}

	return types.EmptyStruct, ErrBranchNotFound
}

func getCommitStForHash(ctx context.Context, db datas.Database, c string) (types.Struct, error) {
	prefixed := c

	if !strings.HasPrefix(c, "#") {
		prefixed = "#" + c
	}

	ap, err := spec.NewAbsolutePath(prefixed)

	if err != nil {
		return types.EmptyStruct, err
	}

	val := ap.Resolve(ctx, db)

	if val == nil {
		return types.EmptyStruct, ErrHashNotFound
	}

	valSt, ok := val.(types.Struct)

	if !ok || valSt.Name() != CommitStructName {
		return types.EmptyStruct, ErrFoundHashNotACommit
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
				return types.EmptyStruct, ErrInvalidAnscestorSpec
			}
			commitSt = *commitStPtr
		} else {
			return types.EmptyStruct, ErrInvalidAnscestorSpec
		}
	}

	return commitSt, nil
}

// Resolve takes a CommitSpec and returns a Commit, or an error if the commit cannot be found.
func (ddb *DoltDB) Resolve(cs *CommitSpec) (*Commit, error) {
	var commitSt types.Struct
	err := pantoerr.PanicToError("unable to resolve commit "+cs.Name(), func() error {
		var err error
		if cs.csType == CommitHashSpec {
			commitSt, err = getCommitStForHash(context.TODO(), ddb.db, cs.Name())
		} else {
			commitSt, err = getCommitStForBranch(context.TODO(), ddb.db, cs.Name())
		}

		if err != nil {
			return err
		}

		commitSt, err = walkAncestorSpec(context.TODO(), ddb.db, commitSt, cs.AncestorSpec())

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
		ddb.db.Flush(context.TODO())

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
func (ddb *DoltDB) Commit(ctx context.Context, valHash hash.Hash, branch string, cm *CommitMeta) (*Commit, error) {
	return ddb.CommitWithParents(ctx, valHash, branch, nil, cm)
}

// FastForward fast-forwards the branch given to the commit given.
func (ddb *DoltDB) FastForward(branch string, commit *Commit) error {
	ds := ddb.db.GetDataset(context.TODO(), branch)
	_, err := ddb.db.FastForward(context.TODO(), ds, types.NewRef(commit.commitSt))

	return err
}

// CanFastForward returns whether the given branch can be fast-forwarded to the commit given.
func (ddb *DoltDB) CanFastForward(branch string, new *Commit) (bool, error) {
	currentSpec, _ := NewCommitSpec("HEAD", branch)
	current, err := ddb.Resolve(currentSpec)

	if err != nil {
		if err == ErrBranchNotFound {
			return true, nil
		}

		return false, err
	}

	return current.CanFastForwardTo(context.TODO(), new)
}

// CommitWithParents commits the value hash given to the branch given, using the list of parent hashes given. Returns an
// error if the value or any parents can't be resolved, or if anything goes wrong accessing the underlying storage.
func (ddb *DoltDB) CommitWithParents(ctx context.Context, valHash hash.Hash, branch string, parentCmSpecs []*CommitSpec, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	err := pantoerr.PanicToError("error committing value "+valHash.String(), func() error {
		val := ddb.db.ReadValue(ctx, valHash)

		if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
			return errors.New("can't commit a value that is not a valid root value")
		}

		ds := ddb.db.GetDataset(context.TODO(), branch)
		parentEditor := types.NewSet(context.TODO(), ddb.db).Edit()
		if ds.HasHead() {
			parentEditor.Insert(ds.HeadRef())
		}

		for _, parentCmSpec := range parentCmSpecs {
			cs, err := ddb.Resolve(parentCmSpec)

			if err != nil {
				return err
			}

			parentEditor.Insert(types.NewRef(cs.commitSt))
		}

		parents := parentEditor.Set(context.TODO())
		commitOpts := datas.CommitOptions{Parents: parents, Meta: cm.toNomsStruct(), Policy: nil}
		ds, err := ddb.db.Commit(context.TODO(), ddb.db.GetDataset(context.TODO(), branch), val, commitOpts)

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
	valRef := types.NewRef(val)

	targetVal := valRef.TargetValue(ctx, vrw)

	if targetVal == nil {
		vrw.WriteValue(ctx, val)
	}

	return valRef
}

// ResolveParent returns the n-th ancestor of a given commit (direct parent is index 0). error return value will be
// non-nil in the case that the commit cannot be resolved, there aren't as many ancestors as requested, or the
// underlying storage cannot be accessed.
func (ddb *DoltDB) ResolveParent(commit *Commit, parentIdx int) (*Commit, error) {
	var parentCommitSt types.Struct
	errMsg := fmt.Sprintf("failed to resolve parent of %s", commit.HashOf().String())
	err := pantoerr.PanicToErrorNil(errMsg, func() {
		parentSet := commit.getParents()
		itr := parentSet.IteratorAt(context.TODO(), uint64(parentIdx))
		parentCommRef := itr.Next(context.TODO())

		parentVal := parentCommRef.(types.Ref).TargetValue(context.TODO(), ddb.ValueReadWriter())
		parentCommitSt = parentVal.(types.Struct)
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.ValueReadWriter(), parentCommitSt}, nil
}

// HasBranch returns whether the branch given exists in this database.
func (ddb *DoltDB) HasBranch(name string) bool {
	return ddb.db.Datasets(context.TODO()).Has(context.TODO(), types.String(name))
}

// GetBranches returns a list of all branches in the database.
func (ddb *DoltDB) GetBranches() []string {
	var branches []string
	ddb.db.Datasets(context.TODO()).IterAll(context.TODO(), func(key, _ types.Value) {
		keyStr := string(key.(types.String))
		if !strings.HasPrefix(keyStr, "__") {
			branches = append(branches, keyStr)
		}
	})

	return branches
}

// NewBranchAtCommit creates a new branch with HEAD at the commit given. Branch names must pass IsValidUserBranchName.
func (ddb *DoltDB) NewBranchAtCommit(newBranchName string, commit *Commit) error {
	if !IsValidUserBranchName(newBranchName) {
		panic(fmt.Sprintf("invalid branch name %v, use IsValidUserBranchName check", newBranchName))
	}

	ds := ddb.db.GetDataset(context.TODO(), newBranchName)
	_, err := ddb.db.SetHead(context.TODO(), ds, types.NewRef(commit.commitSt))
	return err
}

// DeleteBranch deletes the branch given, returning an error if it doesn't exist.
func (ddb *DoltDB) DeleteBranch(branchName string) error {
	if !IsValidUserBranchName(branchName) && !IsValidRemoteBranchName(branchName) {
		panic(fmt.Sprintf("invalid branch name %v, use IsValidUserBranchName check", branchName))
	}

	ds := ddb.db.GetDataset(context.TODO(), branchName)

	if !ds.HasHead() {
		return ErrBranchNotFound
	}

	_, err := ddb.db.Delete(context.TODO(), ds)
	return err
}

// PullChunks initiates a pull into this database from the source database given, at the commit given. The pull occurs
// asynchronously, and progress is communicated over the provided channel.
func (ddb *DoltDB) PullChunks(srcDB *DoltDB, cm *Commit, progChan chan datas.PullProgress) error {
	datas.Pull(context.TODO(), srcDB.db, ddb.db, types.NewRef(cm.commitSt), progChan)
	return nil
}
