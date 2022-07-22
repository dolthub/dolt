package table

import (
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRand = rand.New(rand.NewSource(1))

func TestTableIteratorProlly(t *testing.T) {
	n := 100

	for i := 0; i < 10; i++ {
		offset := testRand.Intn(n)
		m, tups := mustMakeProllyMap(t, n)
		idx := durable.IndexFromProllyMap(m)
		itr, err := NewTableIterator(context.Background(), sch, idx, uint64(offset))
		require.NoError(t, err)
		expectedRows := tuplesToRows(t, tups[offset:])
		testIterator(t, itr, expectedRows)
	}
}

func testIterator(t *testing.T, iter RowIter, expected []sql.Row) {
	ctx := context.Background()
	for _, eR := range expected {
		r, err := iter.Next(ctx)
		require.NoError(t, err)
		assert.Equal(t, eR, r)
	}
	_, err := iter.Next(ctx)
	require.Equal(t, io.EOF, err)
}

var colSqlType = sch.GetAllCols().GetAtIndex(0).TypeInfo.ToSqlType()
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

	ns := tree.NewTestNodeStore()

	tuples := tree.RandomTuplePairs(count, kd, vd, ns)
	om := prolly.MustProllyMapFromTuples(t, kd, vd, tuples)

	return om, tuples
}

func tuplesToRows(t *testing.T, kvs [][2]val.Tuple) (rows []sql.Row) {

	rows = make([]sql.Row, len(kvs))
	for i, kv := range kvs {
		v1, err := index.GetField(context.Background(), kd, 0, kv[0], nil)
		require.NoError(t, err)
		v2, err := index.GetField(context.Background(), kd, 0, kv[1], nil)
		require.NoError(t, err)
		rows[i] = sql.Row{v1, v2}
	}

	return
}
