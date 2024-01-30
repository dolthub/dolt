// Copyright 2024 Dolthub, Inc.
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

package stats

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func newStatsTable(ctx *sql.Context, ns tree.NodeStore, vrw stypes.ValueReadWriter) (*doltdb.Table, error) {
	return doltdb.CreateEmptyTable(ctx, ns, vrw, schema.StatsTableDoltSchema)
}

// flushStats writes a set of table statistics to the given node store, and returns a new prolly.Map
func flushStats(ctx *sql.Context, prev prolly.Map, tableStats map[sql.StatQualifier]*DoltStats) (prolly.Map, error) {
	sch := schema.StatsTableDoltSchema
	kd, vd := sch.GetMapDescriptors()
	var m *prolly.MutableMap
	m = prev.Mutate()
	pool := prev.NodeStore().Pool()

	keyBuilder := val.NewTupleBuilder(kd)
	valueBuilder := val.NewTupleBuilder(vd)

	stringifyKey := func(r sql.Row, types []sql.Type) string {
		b := strings.Builder{}
		sep := ""
		for i, v := range r {
			if v == nil {
				v = types[i].Zero()
			}
			fmt.Fprintf(&b, "%s%v", sep, v)
			sep = ","
		}
		return b.String()
	}
	for qual, stats := range tableStats {
		var pos int64

		// delete previous entries for this index
		keyBuilder.PutString(0, qual.Database)
		keyBuilder.PutString(1, qual.Table())
		keyBuilder.PutString(2, qual.Index())
		keyBuilder.PutInt64(3, 0)
		firstKey := keyBuilder.Build(pool)
		keyBuilder.PutString(0, qual.Database)
		keyBuilder.PutString(1, qual.Table())
		keyBuilder.PutString(2, qual.Index())
		keyBuilder.PutInt64(3, 10000)
		maxKey := keyBuilder.Build(pool)

		// there is a limit on the number of buckets for a given index, iter
		// will terminate after we run over.
		iter, err := prev.IterKeyRange(ctx, firstKey, maxKey)
		if err != nil {
			return prolly.Map{}, err
		}

		for {
			k, _, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return prolly.Map{}, err
			}
			err = m.Put(ctx, k, nil)
			if err != nil {
				return prolly.Map{}, err
			}
		}

		// now add new buckets
		typesB := strings.Builder{}
		sep := ""
		for _, t := range stats.Types {
			typesB.WriteString(sep + t.String())
			sep = ","
		}
		typesStr := typesB.String()

		for _, h := range stats.Histogram {
			var upperBoundElems []string
			for _, v := range h.UpperBound {
				upperBoundElems = append(upperBoundElems, fmt.Sprintf("%v", v))
			}

			keyBuilder.PutString(0, qual.Database)
			keyBuilder.PutString(1, qual.Tab)
			keyBuilder.PutString(2, qual.Idx)
			keyBuilder.PutInt64(3, pos)

			valueBuilder.PutInt64(0, schema.StatsVersion)
			valueBuilder.PutString(1, h.Chunk.String())
			valueBuilder.PutInt64(2, int64(h.RowCount))
			valueBuilder.PutInt64(3, int64(h.DistinctCount))
			valueBuilder.PutInt64(4, int64(h.NullCount))
			valueBuilder.PutString(5, strings.Join(stats.Columns, ","))
			valueBuilder.PutString(6, typesStr)
			valueBuilder.PutString(7, stringifyKey(h.UpperBound, stats.Types))
			valueBuilder.PutInt64(8, int64(h.BoundCount))
			valueBuilder.PutDatetime(9, h.CreatedAt)
			for i, r := range h.Mcvs {
				valueBuilder.PutString(10+i, stringifyKey(r, stats.Types))
			}
			var mcvCntsRow sql.Row
			for _, v := range h.McvCount {
				mcvCntsRow = append(mcvCntsRow, int(v))
			}
			valueBuilder.PutString(14, stringifyKey(mcvCntsRow, stats.Types))

			key := keyBuilder.Build(pool)
			value := valueBuilder.Build(pool)
			m.Put(ctx, key, value)
			pos++
		}
	}

	return m.Map(ctx)
}

func deleteStats(ctx *sql.Context, prev prolly.Map, quals ...sql.StatQualifier) (prolly.Map, error) {
	if cnt, err := prev.Count(); err != nil {
		return prolly.Map{}, err
	} else if cnt == 0 {
		return prev, nil
	}

	sch := schema.StatsTableDoltSchema
	kd, _ := sch.GetMapDescriptors()
	var m *prolly.MutableMap
	m = prev.Mutate()
	pool := prev.NodeStore().Pool()

	keyBuilder := val.NewTupleBuilder(kd)

	for _, qual := range quals {
		// delete previous entries for this index
		keyBuilder.PutString(0, qual.Database)
		keyBuilder.PutString(1, qual.Table())
		keyBuilder.PutString(2, qual.Index())
		keyBuilder.PutInt64(3, 0)
		firstKey := keyBuilder.Build(pool)
		keyBuilder.PutString(0, qual.Database)
		keyBuilder.PutString(1, qual.Table())
		keyBuilder.PutString(2, qual.Index())
		keyBuilder.PutInt64(3, 10000)
		maxKey := keyBuilder.Build(pool)

		// there is a limit on the number of buckets for a given index, iter
		// will terminate after we run over.
		iter, err := prev.IterKeyRange(ctx, firstKey, maxKey)
		if err != nil {
			return prolly.Map{}, err
		}

		for {
			k, _, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return prolly.Map{}, err
			}
			err = m.Put(ctx, k, nil)
			if err != nil {
				return prolly.Map{}, err
			}
		}
	}
	return m.Map(ctx)
}
