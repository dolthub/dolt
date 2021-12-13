// Copyright 2021 Dolthub, Inc.
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

package durable

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type Table interface {
	GetSchemaHash(ctx context.Context) (hash.Hash, error)
	GetSchema(ctx context.Context) (schema.Schema, error)
	SetSchema(ctx context.Context, sch schema.Schema) error

	GetTableRows(ctx context.Context) (types.Map, error)
	SetTableRows(ctx context.Context, rows types.Map) error

	GetIndexes(ctx context.Context) (types.Map, error)
	SetIndexes(ctx context.Context, indexes types.Map) error

	GetConflicts(ctx context.Context) (doltdb.ConflictSchema, types.Map, error)
	SetConflicts(ctx context.Context, sch doltdb.ConflictSchema, conflicts types.Map) error

	GetConstraintViolations(ctx context.Context) (types.Map, error)
	SetConstraintViolations(ctx context.Context, violations types.Map) error

	GetAutoIncrement(ctx context.Context) (types.Value, error)
	SetAutoIncrement(ctx context.Context, val types.Value) error
}
