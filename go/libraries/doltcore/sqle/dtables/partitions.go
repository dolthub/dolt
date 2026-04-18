// Copyright 2026 Dolthub, Inc.
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

package dtables

import (
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// filteredPartition represents a partition whose rows are constrained to a
// lower and upper bound on a string key (e.g. tag_name, branch name).
type filteredPartition struct {
	lowerBound          string
	lowerBoundInclusive bool
	upperBound          string
	upperBoundInclusive bool
}

var _ sql.Partition = (*filteredPartition)(nil)

// Key implements sql.Partition
func (f filteredPartition) Key() []byte {
	// Key is not used to identify the partition, so we return nil
	return nil
}

// SliceOfPartitionsItr is a sql.PartitionIter backed by a pre-built slice of
// partitions. It is safe for concurrent use.
type SliceOfPartitionsItr struct {
	mu         sync.Mutex
	partitions []sql.Partition
	i          int
}

func NewSliceOfPartitionsItr(partitions []sql.Partition) *SliceOfPartitionsItr {
	return &SliceOfPartitionsItr{partitions: partitions}
}

func (itr *SliceOfPartitionsItr) Next(*sql.Context) (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.i >= len(itr.partitions) {
		return nil, io.EOF
	}

	next := itr.partitions[itr.i]
	itr.i++

	return next, nil
}

func (itr *SliceOfPartitionsItr) Close(*sql.Context) error {
	return nil
}

// parseMySQLRangeLookup converts a MySQLRangeCollection index lookup into a
// slice of filteredPartitions covering the requested string-key ranges.
func parseMySQLRangeLookup(lookup sql.IndexLookup) ([]sql.Partition, error) {
	mysqlRanges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported range cut type: %T", lookup.Ranges)
	}

	var partitions []sql.Partition
	for i := range mysqlRanges.ToRanges() {
		mysqlRange := mysqlRanges.ToRanges()[i].(sql.MySQLRange)
		rangeExpr := mysqlRange[0]

		lowerBoundInclusive := false
		noLowerBound := false
		var lower string
		switch x := rangeExpr.LowerBound.(type) {
		case sql.Above:
			lower = x.Key.(string)
		case sql.Below:
			lower = x.Key.(string)
			lowerBoundInclusive = true
		case sql.BelowNull, sql.AboveNull:
			lower = ""
		case sql.AboveAll:
			noLowerBound = true
			lower = ""
		default:
			return nil, fmt.Errorf("unknown range cut type: %T", rangeExpr.LowerBound)
		}

		upperBoundInclusive := false
		noUpperBound := false
		var upper string
		switch x := rangeExpr.UpperBound.(type) {
		case sql.Above:
			upper = x.Key.(string)
			upperBoundInclusive = true
		case sql.Below:
			upper = x.Key.(string)
		case sql.AboveAll:
			noUpperBound = true
			upper = ""
		case sql.BelowNull, sql.AboveNull:
			upper = ""
		default:
			return nil, fmt.Errorf("unknown range cut type: %T", rangeExpr.UpperBound)
		}

		if noUpperBound && noLowerBound {
			continue
		}

		partitions = append(partitions, &filteredPartition{
			lowerBound:          lower,
			lowerBoundInclusive: lowerBoundInclusive,
			upperBound:          upper,
			upperBoundInclusive: upperBoundInclusive,
		})
	}
	return partitions, nil
}

// outOfLowerBound reports whether name falls below the lower bound, meaning it
// should be excluded from results. An empty bound is treated as unbounded.
func outOfLowerBound(name, bound string, inclusive bool) bool {
	if bound == "" {
		return false
	}
	if inclusive {
		return name < bound
	}
	return name <= bound
}

// outOfUpperBound reports whether name falls above the upper bound, meaning it
// should be excluded from results. An empty bound is treated as unbounded.
func outOfUpperBound(name, bound string, inclusive bool) bool {
	if bound == "" {
		return false
	}
	if inclusive {
		return name > bound
	}
	return name >= bound
}
