package prolly

import (
	"context"
	"testing"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/require"
)

func MustProllyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, tuples [][2]val.Tuple) Map {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, pair := range tuples {
		err := chunker.AddPair(ctx, tree.Item(pair[0]), tree.Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return NewMap(root, ns, kd, vd)
}
