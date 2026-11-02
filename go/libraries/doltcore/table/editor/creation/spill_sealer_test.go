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

package creation

import (
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/sort"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func TestBuildIndexAsksForSpillSealer(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v", 1, types.IntKind, false),
	))
	idx, err := sch.Indexes().AddIndexByColNames("v_idx", []string{"v"}, nil, schema.IndexProperties{IsUserDefined: true})
	require.NoError(t, err)
	ns := tree.NewTestNodeStore()
	kd, vd := sch.GetMapDescriptors(ns)
	primary, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	require.NoError(t, err)
	vrw := types.NewMemoryValueStore()

	var asked []types.ValueReadWriter
	errToy := errors.New("toy")
	sealerErr := errToy
	prev := spillSealer
	t.Cleanup(func() { spillSealer = prev })
	spillSealer = func(vrw types.ValueReadWriter) (sort.SpillSealer, error) {
		asked = append(asked, vrw)
		return nil, sealerErr
	}

	_, err = BuildProllyIndexExternal(ctx, vrw, ns, sch, "t", idx, primary, nil, nil)
	assert.ErrorIs(t, err, errToy)

	sealerErr = nil
	_, err = BuildProllyIndexExternal(ctx, vrw, ns, sch, "t", idx, primary, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []types.ValueReadWriter{vrw, vrw}, asked)
}
