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

// filteredPartition is a sql.Partition whose rows are constrained to a
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
	// Dispatch happens via type assertion on the partition, not by key comparison.
	return nil
}

// SliceOfPartitionsItr is a sql.PartitionIter backed by a pre-built slice of partitions.
// It is safe for concurrent use.
type SliceOfPartitionsItr struct {
	mu         sync.Mutex
	partitions []sql.Partition
	i          int
}

// NewSliceOfPartitionsItr returns a PartitionIter over |partitions|.
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

// partitionsFromLookup converts |lookup| into a slice of filteredPartitions
// covering the requested string-key ranges.
func partitionsFromLookup(lookup sql.IndexLookup) ([]sql.Partition, error) {
	mysqlRanges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported range cut type: %T", lookup.Ranges)
	}

	ranges := mysqlRanges.ToRanges()
	var partitions []sql.Partition
	for i := range ranges {
		mysqlRange := ranges[i].(sql.MySQLRange)
		if len(mysqlRange) == 0 {
			return nil, fmt.Errorf("empty MySQLRange at index %d", i)
		}
		rangeExpr := mysqlRange[0]

		lower, lowerInc, noLower, err := parseStringBound(rangeExpr.LowerBound, true)
		if err != nil {
			return nil, err
		}
		upper, upperInc, noUpper, err := parseStringBound(rangeExpr.UpperBound, false)
		if err != nil {
			return nil, err
		}

		if noLower && noUpper {
			continue
		}

		partitions = append(partitions, &filteredPartition{
			lowerBound:          lower,
			lowerBoundInclusive: lowerInc,
			upperBound:          upper,
			upperBoundInclusive: upperInc,
		})
	}
	return partitions, nil
}

// parseStringBound extracts a string value and inclusiveness from |cut|.
// |isLower| is required because Above and Below have opposite inclusiveness
// depending on which side of the range they appear on. Returns |noBound|=true
// when the cut is AboveAll, meaning no values can satisfy the bound.
func parseStringBound(cut sql.MySQLRangeCut, isLower bool) (value string, inclusive bool, noBound bool, err error) {
	switch c := cut.(type) {
	case sql.Above:
		str, ok := c.Key.(string)
		if !ok {
			return "", false, false, fmt.Errorf("expected string range key, got %T", c.Key)
		}
		return str, !isLower, false, nil
	case sql.Below:
		str, ok := c.Key.(string)
		if !ok {
			return "", false, false, fmt.Errorf("expected string range key, got %T", c.Key)
		}
		return str, isLower, false, nil
	case sql.AboveAll:
		return "", false, true, nil
	case sql.BelowNull, sql.AboveNull:
		return "", false, false, nil
	default:
		return "", false, false, fmt.Errorf("unknown range cut type: %T", cut)
	}
}

// outOfLowerBound reports whether |name| falls below |bound| and should be
// excluded from results. An empty |bound| is treated as unbounded.
func outOfLowerBound(name, bound string, inclusive bool) bool {
	if bound == "" {
		return false
	}
	if inclusive {
		return name < bound
	}
	return name <= bound
}

// outOfUpperBound reports whether |name| falls above |bound| and should be
// excluded from results. An empty |bound| is treated as unbounded.
func outOfUpperBound(name, bound string, inclusive bool) bool {
	if bound == "" {
		return false
	}
	if inclusive {
		return name > bound
	}
	return name >= bound
}
