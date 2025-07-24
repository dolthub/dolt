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
	// HasConflicts returns whether the root object contains conflicts.
	HasConflicts(ctx context.Context) (bool, error)
	// HashOf returns the hash of the underlying struct.
	HashOf(ctx context.Context) (hash.Hash, error)
	// Name returns the name of the root object.
	Name() TableName
}

// ConflictRootObject is a RootObject that contains conflict information.
type ConflictRootObject interface {
	RootObject
	// Schema returns the schema that should be displayed.
	Schema(originatingTableName string) sql.Schema
	// Rows returns the rows that represent conflicting portions of a root object.
	Rows(ctx *sql.Context) (sql.RowIter, error)
}
