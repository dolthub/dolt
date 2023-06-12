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

package diff

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDelta interface {
	GetBaseInfo() TableDeltaBase
	IsAdd() bool
	IsDrop() bool
	IsRename() bool
	HasHashChanged() (bool, error)
	HasSchemaChanged(ctx context.Context) (bool, error)
	HasDataChanged(ctx context.Context) (bool, error)
	HasPrimaryKeySetChanged() bool
	HasChanges() (bool, error)
	CurName() string
	HasFKChanges() bool
	GetSchemas(ctx context.Context) (from, to schema.Schema, err error)
	IsKeyless(ctx context.Context) (bool, error)
	GetSummary(ctx context.Context) (*TableDeltaSummary, error)
	GetTableCreateStatement(ctx context.Context, isFromTable bool) (string, error)
}
