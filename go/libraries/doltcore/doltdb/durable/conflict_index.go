// Copyright 2022 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type ConflictIndex interface {
	HashOf() (hash.Hash, error)
	Count() uint64
	Format() *types.NomsBinFormat
}

// RefFromConflictIndex persists |idx| and returns the types.Ref targeting it.
func RefFromConflictIndex(ctx context.Context, vrw types.ValueReadWriter, idx ConflictIndex) (types.Ref, error) {
	switch idx.Format() {
	case types.Format_LD_1:
		panic("Unsupported format: " + idx.Format().VersionString())

	case types.Format_DOLT:
		return types.Ref{}, fmt.Errorf("__DOLT__ conflicts should be stored in ArtifactIndex")

	default:
		return types.Ref{}, errNbfUnknown
	}
}

// NewEmptyConflictIndex returns an ConflictIndex with no rows.
func NewEmptyConflictIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, oursSch, theirsSch, baseSch schema.Schema) (ConflictIndex, error) {
	switch vrw.Format() {
	case types.Format_LD_1:
		panic("Unsupported format: " + vrw.Format().VersionString())

	case types.Format_DOLT:
		return nil, fmt.Errorf("__DOLT__ conflicts should be stored in ArtifactIndex")

	default:
		return nil, errNbfUnknown
	}
}
