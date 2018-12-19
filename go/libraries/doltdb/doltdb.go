package doltdb

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"strings"
)

const (
	creationBranch = "__create__"
	MasterBranch   = "master"
)

// DoltDBLocation represents a locations where a DoltDB database lives.
type DoltDBLocation string

const (
	// InMemDoltDB stores the DoltDB db in memory and is primarily used for testing
	InMemDoltDB = DoltDBLocation("mem")

	// LocalDirDoltDB stores the db in the current directory
	LocalDirDoltDB = DoltDBLocation("nbs:./")
)

// DoltDB wraps access to the underlying noms database and hides some of the details of the underlying storage.
// Additionally the noms codebase uses panics in a way that is non idiomatic and I've opted to recover and return
// errors in many cases.
type DoltDB struct {
	db datas.Database
}

// LoadDoltDB will acquire a reference to the underlying noms db.  If the DoltDBLocation is InMemDoltDB then a reference
// to a newly created in memory database will be used, if the location is LocalDirDoltDB the current working directory
// will be used for the db.
func LoadDoltDB(loc DoltDBLocation) *DoltDB {
	dbSpec, _ := spec.ForDatabase(string(loc))

	// There is the possibility of this panicking, but have decided specifically not to recover (as is normally done in
	// this codebase. For failure to occur getting a database for the current directory, or an in memory database
	// something would have to be drastically wrong.
	db := dbSpec.GetDatabase()
	return &DoltDB{db}
}

// WriteEmptyRepo will create initialize the given db with a master branch which points to a commit which has valid
// metadata for the creation commit, and an empty RootValue.
func (ddb *DoltDB) WriteEmptyRepo(name, email string) error {
	if ddb.db.GetDataset(creationBranch).HasHead() {
		return errors.New("Database already exists")
	}

	name = strings.TrimSpace(name)
	email = strings.TrimSpace(email)

	if name == "" || email == "" {
		panic("Passed bad name or email.  Both should be valid")
	}

	err := errhand.PanicToError("Failed to write empty repo", func() error {
		rv := emptyRootValue(ddb.db)
		_, err := ddb.WriteRootValue(rv)

		cm := NewCommitMeta(name, email, "Data repository created.")

		commitOpts := datas.CommitOptions{Parents: types.Set{}, Meta: cm.toNomsStruct(), Policy: nil}
		firstCommit, err := ddb.db.Commit(ddb.db.GetDataset(creationBranch), rv.valueSt, commitOpts)

		if err != nil {
			return err
		}

		_, err = ddb.db.SetHead(ddb.db.GetDataset(MasterBranch), firstCommit.HeadRef())

		return err
	})

	return err
}

func getCommitStForBranch(db datas.Database, b string) (types.Struct, error) {
	ds := db.GetDataset(b)

	if ds.HasHead() {
		return ds.Head(), nil
	}

	return types.EmptyStruct, errors.New("Could find branch " + b)
}

func getCommitStForHash(db datas.Database, c string) (types.Struct, error) {
	prefixed := c

	if !strings.HasPrefix(c, "#") {
		prefixed = "#" + c
	}

	ap, err := spec.NewAbsolutePath(prefixed)

	if err != nil {
		return types.EmptyStruct, err
	}

	return ap.Resolve(db).(types.Struct), nil
}

var ErrInvalidAnscestorSpec = errors.New("invalid anscestor spec")

func walkAncestorSpec(db datas.Database, commitSt types.Struct, aSpec *AncestorSpec) (types.Struct, error) {
	if aSpec == nil || len(aSpec.Instructions) == 0 {
		return commitSt, nil
	}

	instructions := aSpec.Instructions
	for _, inst := range instructions {
		cm := Commit{db, commitSt}

		if inst < cm.NumParents() {
			commitStPtr := cm.getParent(inst)

			if commitStPtr == nil {
				return types.EmptyStruct, ErrInvalidAnscestorSpec
			} else {
				commitSt = *commitStPtr
			}
		} else {
			return types.EmptyStruct, ErrInvalidAnscestorSpec
		}
	}

	return commitSt, nil
}

// Resolve takes a CommitSpec and, if it exists, returns a Commit instance.
func (ddb *DoltDB) Resolve(cs *CommitSpec) (*Commit, error) {
	var commitSt types.Struct
	err := errhand.PanicToError("Unable to resolve commit "+cs.Name(), func() error {
		getCFunc := getCommitStForBranch
		if cs.csType == commitHashSpec {
			getCFunc = getCommitStForHash
		}

		var err error
		commitSt, err = getCFunc(ddb.db, cs.Name())

		if err != nil {
			return err
		}

		commitSt, err = walkAncestorSpec(ddb.db, commitSt, cs.AncestorSpec())

		return err
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
}

// WriteRootValue will write an doltdb.RootValue instance to the database.  This value will not be associated with a commit
// and can be committed by hash at a later time.
func (ddb *DoltDB) WriteRootValue(rv *RootValue) (hash.Hash, error) {
	var valHash hash.Hash
	err := errhand.PanicToError("Failed to write value.", func() error {
		ref := ddb.db.WriteValue(rv.valueSt)
		ddb.db.Flush()

		valHash = ref.TargetHash()
		return nil
	})

	return valHash, err
}

func (ddb *DoltDB) ReadRootValue(h hash.Hash) (*RootValue, error) {
	var val types.Value
	err := errhand.PanicToError("Unable to read root value.", func() error {
		val = ddb.db.ReadValue(h)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if val != nil {
		if rootSt, ok := val.(types.Struct); ok {
			return &RootValue{ddb.db, rootSt}, nil
		}
	}

	return nil, errors.New("There is no dolt root value at that hash.")
}

// Commit will update a branch's head value to be that of a previously committed root value hash
func (ddb *DoltDB) Commit(valHash hash.Hash, branch string, cm *CommitMeta) (*Commit, error) {
	var commitSt types.Struct
	err := errhand.PanicToError("Error committing value "+valHash.String(), func() error {
		val := ddb.db.ReadValue(valHash)

		if st, ok := val.(types.Struct); !ok || st.Name() != ddbRootStructName {
			return errors.New("Can't commit a value that is not a valid root value.")
		}

		ds := ddb.db.GetDataset(branch)
		parents := types.Set{}
		if ds.HasHead() {
			parents = types.NewSet(ddb.db, ds.HeadRef())
		}

		commitOpts := datas.CommitOptions{Parents: parents, Meta: cm.toNomsStruct(), Policy: nil}
		ds, err := ddb.db.Commit(ddb.db.GetDataset(branch), val, commitOpts)

		if ds.HasHead() {
			commitSt = ds.Head()
		} else if err == nil {
			return errors.New("Commit has no head but commit succeeded?!?! How?")
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.db, commitSt}, nil
}

func (ddb *DoltDB) ValueReadWriter() types.ValueReadWriter {
	return ddb.db
}

func writeValAndGetRef(vrw types.ValueReadWriter, val types.Value) types.Ref {
	valRef := types.NewRef(val)

	targetVal := valRef.TargetValue(vrw)

	if targetVal == nil {
		vrw.WriteValue(val)
	}

	return valRef
}

func (ddb *DoltDB) ResolveParent(commit *Commit, parentIdx int) (*Commit, error) {
	var parentCommitSt types.Struct
	errMsg := fmt.Sprintf("Failed to resolve parent of %s", commit.HashOf().String())
	err := errhand.PanicToError(errMsg, func() error {
		parentSet := commit.getParents()
		itr := parentSet.IteratorAt(uint64(parentIdx))
		parentCommRef := itr.Next()

		parentVal := parentCommRef.(types.Ref).TargetValue(ddb.ValueReadWriter())
		parentCommitSt = parentVal.(types.Struct)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &Commit{ddb.ValueReadWriter(), parentCommitSt}, nil
}
