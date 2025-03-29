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

package table

import (
	"io"
	"math/rand"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var testRand = rand.New(rand.NewSource(1))

func TestTableIteratorProlly(t *testing.T) {
	n := 100

	m, tups := mustMakeProllyMap(t, n)
	idx := durable.IndexFromProllyMap(m)
	itr, err := NewTableIterator(sql.NewEmptyContext(), sch, idx)
	require.NoError(t, err)
	expectedRows := tuplesToRows(t, tups)
	testIterator(t, itr, expectedRows)
}

func testIterator(t *testing.T, iter RowIter, expected []sql.Row) {
	ctx := sql.NewEmptyContext()
	for _, eR := range expected {
		r, err := iter.Next(ctx)
		require.NoError(t, err)
		assert.Equal(t, eR, r)
	}
	_, err := iter.Next(ctx)
	require.Equal(t, io.EOF, err)
}

var sch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk", 0, types.UintKind, true),
	schema.NewColumn("col1", 1, types.UintKind, false)))
var kd = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: false},
)
var vd = val.NewTupleDescriptor(
	val.Type{Enc: val.Uint32Enc, Nullable: true},
)

func mustMakeProllyMap(t *testing.T, count int) (prolly.Map, [][2]val.Tuple) {
	ctx := sql.NewEmptyContext()

	ns := tree.NewTestNodeStore()

	tuples, err := tree.RandomTuplePairs(ctx, count, kd, vd, ns)
	require.NoError(t, err)
	om := mustProllyMapFromTuples(t, kd, vd, tuples)

	return om, tuples
}

func mustProllyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, tuples [][2]val.Tuple) prolly.Map {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()

	serializer := message.NewProllyMapSerializer(vd, ns.Pool())
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, pair := range tuples {
		err := chunker.AddPair(ctx, tree.Item(pair[0]), tree.Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return prolly.NewMap(root, ns, kd, vd)
}

func tuplesToRows(t *testing.T, kvs [][2]val.Tuple) (rows []sql.Row) {

	rows = make([]sql.Row, len(kvs))
	for i, kv := range kvs {
		v1, err := tree.GetField(sql.NewEmptyContext(), kd, 0, kv[0], nil)
		require.NoError(t, err)
		v2, err := tree.GetField(sql.NewEmptyContext(), kd, 0, kv[1], nil)
		require.NoError(t, err)
		rows[i] = sql.Row{v1, v2}
	}

	return
}
