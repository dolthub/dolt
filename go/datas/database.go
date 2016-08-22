// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package datas defines and implements the database layer used in Noms.
package datas

import (
	"io"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// Database provides versioned storage for noms values. Each Database instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new Database, representing a new moment in history.
type Database interface {
	// To implement types.ValueWriter, Database implementations provide WriteValue(). WriteValue() writes v to this Database, though v is not guaranteed to be be persistent until after a subsequent Commit(). The return value is the Ref of v.
	types.ValueReadWriter
	io.Closer

	// MaybeHead returns the current Head Commit of this Database, which contains the current root of the Database's value tree, if available. If not, it returns a new Commit and 'false'.
	MaybeHead(datasetID string) (types.Struct, bool)

	// MaybeHeadRef returns the types.Ref of the Head Commit of this Database, and true, if available. If not, it returns an invalid types.Ref and false.
	MaybeHeadRef(datasetID string) (types.Ref, bool)

	// Head returns the current head Commit, which contains the current root of the Database's value tree.
	Head(datasetID string) types.Struct

	// HeadRef returns the ref of the current head Commit. See Head(datasetID).
	HeadRef(datasetID string) types.Ref

	// Datasets returns the root of the database which is a MapOfStringToRefOfCommit where string is a datasetID.
	Datasets() types.Map

	// Commit updates the Commit that datasetID in this database points at. All Values that have been written to this Database are guaranteed to be persistent after Commit(). If the update cannot be performed, e.g., because of a conflict, error will be non-nil. The newest snapshot of the database is always returned.
	Commit(datasetID string, commit types.Struct) (Database, error)

	// Delete removes the Dataset named datasetID from the map at the root of the Database. The Dataset data is not necessarily cleaned up at this time, but may be garbage collected in the future. If the update cannot be performed, e.g., because of a conflict, error will non-nil. The newest snapshot of the database is always returned.
	Delete(datasetID string) (Database, error)

	// SetHead sets the Commit that datasetID in this database points at. All Values that have been written to this Database are guaranteed to be persistent after SetHead(). If the update cannot be performed, e.g., because of a conflict, error will be non-nil. The newest snapshot of the database is always returned.
	SetHead(datasetID string, commit types.Struct) (Database, error)

	has(hash hash.Hash) bool
	validatingBatchStore() types.BatchStore
}

func NewDatabase(cs chunks.ChunkStore) Database {
	return newLocalDatabase(cs)
}
