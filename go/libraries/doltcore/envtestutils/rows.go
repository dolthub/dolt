// Copyright 2019 Liquidata, Inc.
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

package envtestutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// MustRowData converts an array of row.TaggedValues into a map containing that data.
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
