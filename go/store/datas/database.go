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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package datas defines and implements the database layer used in Noms.
package datas

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type DatasetsMap interface {
	// How many datasets are in the map
	Len() (uint64, error)

	IterAll(ctx context.Context, cb func(id string, addr hash.Hash) error) error
}

// Database provides versioned storage for noms values. While Values can be
// directly read and written from a Database, it is generally more appropriate
// to read data by inspecting the Head of a Dataset and write new data by
// updating the Head of a Dataset via Commit() or similar. Particularly, new
// data is not guaranteed to be persistent until after a Commit (Delete,
// SetHeadToCommit, or FastForward) operation completes.
// The Database API is stateful, meaning that calls to GetDataset() or
// Datasets() occurring after a call to Commit() (et al) will represent the
// result of the Commit().
type Database interface {
	// Close must have no side-effects
	io.Closer

	// Datasets returns the root of the database which is a
	// Map<String, Ref<Commit>> where string is a datasetID.
	Datasets(ctx context.Context) (DatasetsMap, error)

	// GetDataset returns a Dataset struct containing the current mapping of
	// datasetID in the above Datasets Map.
	GetDataset(ctx context.Context, datasetID string) (Dataset, error)

	// DatasetsByRootHash returns all datasets as of the root hash given
	DatasetsByRootHash(ctx context.Context, rootHash hash.Hash) (DatasetsMap, error)

	// GetDatasetByRootHash returns a Dataset struct containing the mapping of datasetId in the Datasets map as it
	// existed at the root hash given.
	GetDatasetByRootHash(ctx context.Context, datasetID string, rootHash hash.Hash) (Dataset, error)

	// BuildNewCommit creates a new Commit struct for the provided dataset,
	// but does not modify the dataset. This allows the commit to be inspected
	// if necessary before any update is performed.
	BuildNewCommit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (*Commit, error)

	// Commit updates the Commit that ds.ID() in this database points at. All
	// Values that have been written to this Database are guaranteed to be
	// persistent after Commit() returns successfully.
	//
	// The new Commit struct is constructed using v, opts.ParentsList, and
	// opts.Meta. If opts.ParentsList is empty then the head value from
	// |ds| is used as the parent. If opts.Meta is the zero value the
	// Commit struct as an empty Meta struct.
	//
	// If the update cannot be performed because the existing dataset head
	// is not a common ancestor of the constructed commit struct, returns
	// an 'ErrMergeNeeded' error.
	Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error)

	// WriteCommit has the same behavior as Commit but accepts an already-constructed Commit
	// instead of constructing one from a Value and CommitOptions
	WriteCommit(ctx context.Context, ds Dataset, commit *Commit) (Dataset, error)

	// Tag stores an immutable reference to a Commit. It takes a Hash to
	// the Commit and a Dataset whose head must be nil (ie a newly created
	// Dataset).  The new Tag struct is constructed pointing at
	// |commitAddr| and metadata about the tag contained in the struct
	// `opts.Meta`.
	Tag(ctx context.Context, ds Dataset, commitAddr hash.Hash, opts TagOptions) (Dataset, error)

	// UpdateStashList updates the stash list dataset only with given address hash to the updated stash list.
	// The new/updated stash list address should be obtained before calling this function depending on
	// whether add or remove a stash actions have been performed. This function does not perform any actions
	// on the stash list itself.
	UpdateStashList(ctx context.Context, ds Dataset, stashListAddr hash.Hash) (Dataset, error)

	// SetStatsRef updates the singleton statisics ref for this database.
	SetStatsRef(context.Context, Dataset, hash.Hash) (Dataset, error)

	// UpdateWorkingSet updates the dataset given, setting its value to a new
	// working set value object with the ref and meta given. If the dataset given
	// already had a value, it must match the hash given or this method returns
	// ErrOptimisticLockFailed and the caller must retry.
	// The returned Dataset is always the newest snapshot, regardless of
	// success or failure, and Datasets() is updated to match backing storage
	// upon return as well.
	UpdateWorkingSet(ctx context.Context, ds Dataset, workingSet WorkingSetSpec, prevHash hash.Hash) (Dataset, error)

	// CommitWithWorkingSet combines Commit and UpdateWorkingSet, combining the parameters of both. It uses the
	// pessimistic lock that UpdateWorkingSet does, asserting that the hash |prevWsHash| given is still the current one
	// before attempting to write a new value. And it does the normal optimistic locking that Commit does, assuming the
	// pessimistic locking passes. After this method runs, the two datasets given in |commitDS and |workingSetDS| are both
	// updated in the new root, or neither of them are.
	CommitWithWorkingSet(ctx context.Context, commitDS, workingSetDS Dataset, val types.Value, workingSetSpec WorkingSetSpec, prevWsHash hash.Hash, opts CommitOptions) (Dataset, Dataset, error)

	// Delete removes the Dataset named ds.ID() from the map at the root of
	// the Database. If the Dataset is already not present in the map,
	// returns success. If a workinset path is provided, the Delete will also verify that the working set doesn't
	// have changes which will be lost by the Delete before deleting it as well. If an empty working set path is
	// provided, then no changes to the working set will be made.
	//
	// If the update cannot be performed, e.g., because of a conflict,
	// Delete returns an 'ErrMergeNeeded' error.
	Delete(ctx context.Context, ds Dataset, workingSetPath string) (Dataset, error)

	// SetHead ignores any lineage constraints (e.g. the current head being
	// an ancestor of the new Commit) and force-sets a mapping from
	// datasetID: addr in this database. addr can point to a Commit or a
	// Tag, but if Dataset is already present in the Database, it must
	// point to the type of struct.
	//
	// All values that have been written to this Database are guaranteed to
	// be persistent after SetHead(). If the update cannot be performed,
	// error will be non-nil.
	// The workingSetPath is the path to the working set that should be used for updating the working set. If one
	// is not provided, no working set update will be performed.
	SetHead(ctx context.Context, ds Dataset, newHeadAddr hash.Hash, workingSetPath string) (Dataset, error)

	// FastForward takes a types.Ref to a Commit object and makes it the new
	// Head of ds iff it is a descendant of the current Head. Intended to be
	// used e.g. after a call to Pull(). If the update cannot be performed,
	// e.g., because another process moved the current Head out from under
	// you, err will be non-nil.
	// The workingSetPath is the path to the working set that should be used for updating the working set. If one
	// is not provided, no working set update will be performed.
	FastForward(ctx context.Context, ds Dataset, newHeadAddr hash.Hash, workingSetPath string) (Dataset, error)

	// Stats may return some kind of struct that reports statistics about the
	// ChunkStore that backs this Database instance. The type is
	// implementation-dependent, and impls may return nil
	Stats() interface{}

	// StatsSummary may return a string containing summarized statistics for
	// the ChunkStore that backs this Database. It must return "Unsupported"
	// if this operation is not supported.
	StatsSummary() string

	Format() *types.NomsBinFormat

	// PersistGhostCommitIDs persists the given set of ghost commit IDs to the storage layer of the database. Ghost
	// commits are commits which are real but have not been replicated to this instance of the database. Currently,
	// it is only appropriate to use this method during a shallow clone operation.
	PersistGhostCommitIDs(ctx context.Context, ghosts hash.HashSet) error

	// chunkStore returns the ChunkStore used to read and write
	// groups of values to the database efficiently. This interface is a low-
	// level detail of the database that should infrequently be needed by
	// clients.
	chunkStore() chunks.ChunkStore
}

func NewDatabase(cs chunks.ChunkStore) Database {
	vs := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	return newDatabase(vs, ns)
}

func NewTypesDatabase(vs *types.ValueStore, ns tree.NodeStore) Database {
	return newDatabase(vs, ns)
}

// GarbageCollector provides a method to remove unreferenced data from a store.
type GarbageCollector interface {
	types.ValueReadWriter

	// GC traverses the database starting at the Root and removes
	// all unreferenced data from persistent storage.
	GC(ctx context.Context, mode types.GCMode, oldGenRefs, newGenRefs hash.HashSet, safepointF func() error) error
}

// CanUsePuller returns true if a datas.Puller can be used to pull data from one Database into another.  Not all
// Databases support this yet.
func CanUsePuller(db Database) bool {
	cs := db.chunkStore()
	if tfs, ok := cs.(chunks.TableFileStore); ok {
		ops := tfs.SupportedOperations()
		return ops.CanRead && ops.CanWrite
	}
	return false
}

func GetCSStatSummaryForDB(db Database) string {
	cs := db.chunkStore()
	return cs.StatsSummary()
}

func ChunkStoreFromDatabase(db Database) chunks.ChunkStore {
	return db.chunkStore()
}
