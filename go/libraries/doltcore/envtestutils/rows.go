package envtestutils

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func MustRowData(t *testing.T, ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, colVals []row.TaggedValues) *types.Map {
	m, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)

	me := m.Edit()
	for _, taggedVals := range colVals {
		r, err := row.New(types.Format_Default, sch, taggedVals)
		require.NoError(t, err)

		me = me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = me.Map(ctx)
	require.NoError(t, err)

	return &m
}
