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

package writer

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMappedRowSize(t *testing.T) {
	tests := []struct {
		name     string
		mappings []val.OrdinalMapping
		expected int
	}{
		{name: "no mappings", expected: 0},
		{name: "one empty mapping", mappings: []val.OrdinalMapping{{}}, expected: 0},
		{name: "multiple empty mappings", mappings: []val.OrdinalMapping{{}, {}}, expected: 0},
		{name: "empty mixed with non-empty", mappings: []val.OrdinalMapping{{}, {2}, {}}, expected: 3},
		{name: "identity", mappings: []val.OrdinalMapping{{0, 1, 2}}, expected: 3},
		{name: "hidden gap", mappings: []val.OrdinalMapping{{0}, {1, 3}}, expected: 4},
		{name: "multiple hidden gaps", mappings: []val.OrdinalMapping{{0}, {1, 4}}, expected: 5},
		{name: "highest ordinal in first mapping", mappings: []val.OrdinalMapping{{5}, {1, 2}}, expected: 6},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, mappedRowSize(test.mappings...))
		})
	}
}

func TestIsNoopUpdate(t *testing.T) {
	keyMap := val.OrdinalMapping{0}
	require.True(t, isNoopUpdate(sql.Row{int64(1)}, sql.Row{int64(1)}, keyMap))
	require.False(t, isNoopUpdate(sql.Row{int64(1)}, sql.Row{int64(2)}, keyMap))
	require.True(t, isNoopUpdate(sql.Row{[]byte{1}}, sql.Row{[]byte{1}}, keyMap))
	require.False(t, isNoopUpdate(sql.Row{[]byte{1}}, sql.Row{[]byte{2}}, keyMap))
	require.True(t, isNoopUpdate(sql.Row{nil}, sql.Row{nil}, keyMap))
	require.False(t, isNoopUpdate(sql.Row{nil}, sql.Row{int64(1)}, keyMap))
	require.False(t, isNoopUpdate(sql.Row{int64(1)}, sql.Row{nil}, keyMap))
	require.True(t, isNoopUpdate(sql.Row{[]float32{1}}, sql.Row{[]float32{1}}, keyMap))
	require.False(t, isNoopUpdate(sql.Row{[]float32{1}}, sql.Row{[]float32{2}}, keyMap))
	// Uncomparable values must not panic, and are treated as changed.
	require.False(t, isNoopUpdate(sql.Row{map[string]int{}}, sql.Row{map[string]int{}}, keyMap))
}
