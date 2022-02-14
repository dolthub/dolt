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

package prolly

import (
	"context"
	"io"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

// orderedMap is a utility type that allows us to create a common test
// harness for Map, memoryMap, and MutableMap.
type orderedMap interface {
	Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error)
	IterAll(ctx context.Context) (MapRangeIter, error)
	IterRange(ctx context.Context, rng Range) (MapRangeIter, error)
}

var _ orderedMap = Map{}
var _ orderedMap = MutableMap{}
var _ orderedMap = memoryMap{}

func countOrderedMap(t *testing.T, om orderedMap) (cnt int) {
	iter, err := om.IterAll(context.Background())
	require.NoError(t, err)
	for {
		_, _, err = iter.Next(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cnt++
	}
	return cnt
}

func keyDescFromMap(om orderedMap) val.TupleDesc {
	switch m := om.(type) {
	case Map:
		return m.keyDesc
	case MutableMap:
		return m.prolly.keyDesc
	case memoryMap:
		return m.keyDesc
	default:
		panic("unknown ordered map")
	}
}

func randomTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valBuilder := val.NewTupleBuilder(valDesc)

	items = make([][2]val.Tuple, count)
	for i := range items {
		items[i][0] = randomTuple(keyBuilder)
		items[i][1] = randomTuple(valBuilder)
	}

	dupes := make([]int, 0, count)
	for {
		sortTuplePairs(items, keyDesc)
		for i := range items {
			if i == 0 {
				continue
			}
			if keyDesc.Compare(items[i][0], items[i-1][0]) == 0 {
				dupes = append(dupes, i)
			}
		}
		if len(dupes) == 0 {
			break
		}

		// replace duplicates and validate again
		for _, d := range dupes {
			items[d][0] = randomTuple(keyBuilder)
		}
		dupes = dupes[:0]
	}
	return items
}

func randomCompositeTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	// preconditions
	if count%5 != 0 {
		panic("expected empty divisible by 5")
	}
	if len(keyDesc.Types) < 2 {
		panic("expected composite key")
	}

	tt := randomTuplePairs(count, keyDesc, valDesc)

	tuples := make([][2]val.Tuple, len(tt)*3)
	for i := range tuples {
		j := i % len(tt)
		tuples[i] = tt[j]
	}

	// permute the second column
	swap := make([]byte, len(tuples[0][0].GetField(1)))
	rand.Shuffle(len(tuples), func(i, j int) {
		f1 := tuples[i][0].GetField(1)
		f2 := tuples[i][0].GetField(1)
		copy(swap, f1)
		copy(f1, f2)
		copy(f2, swap)
	})

	sortTuplePairs(tuples, keyDesc)

	tuples = deduplicateTuples(keyDesc, tuples)

	return tuples[:count]
}

// assumes a sorted list
func deduplicateTuples(desc val.TupleDesc, tups [][2]val.Tuple) (uniq [][2]val.Tuple) {
	uniq = make([][2]val.Tuple, 1, len(tups))
	uniq[0] = tups[0]

	for i := 1; i < len(tups); i++ {
		cmp := desc.Compare(tups[i-1][0], tups[i][0])
		if cmp < 0 {
			uniq = append(uniq, tups[i])
		}
	}
	return
}

func randomTuple(tb *val.TupleBuilder) (tup val.Tuple) {
	for i, typ := range tb.Desc.Types {
		randomField(tb, i, typ)
	}
	return tb.Build(sharedPool)
}

func cloneRandomTuples(items [][2]val.Tuple) (clone [][2]val.Tuple) {
	clone = make([][2]val.Tuple, len(items))
	for i := range clone {
		clone[i] = items[i]
	}
	return
}

func sortTuplePairs(items [][2]val.Tuple, keyDesc val.TupleDesc) {
	sort.Slice(items, func(i, j int) bool {
		return keyDesc.Compare(items[i][0], items[j][0]) < 0
	})
}

func shuffleTuplePairs(items [][2]val.Tuple) {
	testRand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
}

func randomField(tb *val.TupleBuilder, idx int, typ val.Type) {
	// todo(andy): add NULLs

	neg := -1
	if testRand.Int()%2 == 1 {
		neg = 1
	}

	switch typ.Enc {
	case val.Int8Enc:
		v := int8(testRand.Intn(math.MaxInt8) * neg)
		tb.PutInt8(idx, v)
	case val.Uint8Enc:
		v := uint8(testRand.Intn(math.MaxUint8))
		tb.PutUint8(idx, v)
	case val.Int16Enc:
		v := int16(testRand.Intn(math.MaxInt16) * neg)
		tb.PutInt16(idx, v)
	case val.Uint16Enc:
		v := uint16(testRand.Intn(math.MaxUint16))
		tb.PutUint16(idx, v)
	case val.Int32Enc:
		v := int32(testRand.Intn(math.MaxInt32) * neg)
		tb.PutInt32(idx, v)
	case val.Uint32Enc:
		v := uint32(testRand.Intn(math.MaxUint32))
		tb.PutUint32(idx, v)
	case val.Int64Enc:
		v := int64(testRand.Intn(math.MaxInt64) * neg)
		tb.PutInt64(idx, v)
	case val.Uint64Enc:
		v := uint64(testRand.Uint64())
		tb.PutUint64(idx, v)
	case val.Float32Enc:
		tb.PutFloat32(idx, testRand.Float32())
	case val.Float64Enc:
		tb.PutFloat64(idx, testRand.Float64())
	case val.StringEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutString(idx, string(buf))
	case val.BytesEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutBytes(idx, buf)
	default:
		panic("unknown encoding")
	}
}

func fmtTupleList(tuples [][2]val.Tuple, kd, vd val.TupleDesc) string {
	var sb strings.Builder
	sb.WriteString("{ ")
	for _, kv := range tuples {
		if kv[0] == nil || kv[1] == nil {
			break
		}
		sb.WriteString(kd.Format(kv[0]))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(kv[1]))
		sb.WriteString(", ")
	}
	sb.WriteString("}")
	return sb.String()
}
