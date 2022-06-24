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

package diff

import (
	"context"
	"time"

	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

// ChangeType is an enum that represents the type of change in a diff
type ChangeType int

const (
	// None is no change
	None ChangeType = iota

	// Added is the ChangeType value for a row that was newly added (In new, but not in old)
	Added

	// Removed is the ChangeTypeProp value for a row that was newly deleted (In old, but not in new)
	Removed

	// ModifiedOld is the ChangeType value for the row which represents the old value of the row before it was changed.
	ModifiedOld

	// ModifiedNew is the ChangeType value for the row which represents the new value of the row after it was changed.
	ModifiedNew
)

type RowDiffer interface {
	// Start starts the RowDiffer.
	Start(ctx context.Context, from, to types.Map)

	// StartWithRange starts the RowDiffer with the specified range
	StartWithRange(ctx context.Context, from, to types.Map, start types.Value, inRange types.ValueInRange)

	// GetDiffs returns the requested number of diff.Differences, or times out.
	GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error)

	// GetDiffsWithFilter returns the requested number of filtered diff.Differences, or times out.
	GetDiffsWithFilter(numDiffs int, timeout time.Duration, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error)

	// Close closes the RowDiffer.
	Close() error
}

// SqlRowDiffWriter knows how to write diff rows to an arbitrary format and destination.
type SqlRowDiffWriter interface {
	// WriteRow writes the diff row given, of the diff type provided. colDiffTypes is guaranteed to be the same length as
	// the input row.
	WriteRow(ctx context.Context, row sql.Row, diffType ChangeType, colDiffTypes []ChangeType) error

	// Close finalizes the work of this writer.
	Close(ctx context.Context) error
}

// ColorFunc is a function that can color a format string
type ColorFunc func (a ...interface{}) string