// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/go-mysql-server/sql"
)

// RootObject is an object that is located on the root, rather than inside another object (table, schema, etc.). This is
// primarily used by Doltgres to store root-level objects (sequences, functions, etc.).
type RootObject interface {
	// HashOf returns the hash of the underlying struct.
	HashOf(ctx context.Context) (hash.Hash, error)
	// Name returns the name of the root object.
	Name() TableName
}

// ConflictRootObject is a RootObject that contains conflict information.
type ConflictRootObject interface {
	RootObject
	// DiffCount returns the number of diffs that this conflict will return.
	DiffCount(ctx *sql.Context) (int, error)
	// RemoveDiffs removes the conflicts indicated by the given diffs. Returns an updated root object that will be
	// written to the root.
	RemoveDiffs(ctx *sql.Context, diffs []RootObjectDiff) (ConflictRootObject, error)
	// Rows returns the rows that represent conflicting portions of a root object. These should be able to be converted
	// to a RootObjectDiff via FromRow.
	Rows(ctx *sql.Context) (sql.RowIter, error)
	// Schema returns the schema that should be displayed.
	Schema(originatingTableName string) sql.Schema
	// UpdateField updates the root object by applying the change between the two diffs. Returns an updated root object
	// that will be written to the root.
	UpdateField(ctx *sql.Context, oldDiff RootObjectDiff, newDiff RootObjectDiff) (ConflictRootObject, error)
}

// RootObjectDiff is a diff for the RootObject.
type RootObjectDiff interface {
	// CompareIds returns an integer from -1 to 1, with 0 meaning the diffs are equal to each other. This only compares the
	// unique identifier for the diff, so different diffs may return that they are equal.
	CompareIds(ctx context.Context, other RootObjectDiff) (int, error)
	// ToRow returns the diff as a row.
	ToRow(ctx *sql.Context) (sql.Row, error)
}

// RootObjectDiffFromRow returns a diff from the given row.
var RootObjectDiffFromRow = func(ctx *sql.Context, conflict ConflictRootObject, row sql.Row) (RootObjectDiff, error) {
	panic("Dolt impl used")
}
