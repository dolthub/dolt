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

package valuefile

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestReadWriteValueFile(t *testing.T) {
	const numMaps = 1
	const numMapValues = 1

	ctx := context.Background()
	store, err := NewFileValueStore(types.Format_7_18)
	require.NoError(t, err)

	var values []types.Value
	for i := 0; i < numMaps; i++ {
		var kvs []types.Value
		for j := 0; j < numMapValues; j++ {
			kvs = append(kvs, types.Int(j), types.String(uuid.New().String()))
		}
		m, err := types.NewMap(ctx, store, kvs...)
		require.NoError(t, err)

		values = append(values, m)
	}

	path := filepath.Join(os.TempDir(), "file.nvf")
	err = WriteValueFile(ctx, path, store, values...)
	require.NoError(t, err)

	results, err := ReadValueFile(ctx, path)
	require.NoError(t, err)
	require.Equal(t, len(values), len(results))

	for i := 0; i < len(values); i++ {
		require.True(t, values[i].Equals(results[i]))
	}
}
