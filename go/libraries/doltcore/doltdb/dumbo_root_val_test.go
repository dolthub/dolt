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

package doltdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// withFileID returns a copy of |msg| with its flatbuffer file identifier replaced by |fileID|.
func withFileID(msg types.SerialMessage, fileID string) types.SerialMessage {
	out := make(types.SerialMessage, len(msg))
	copy(out, msg)
	copy(out[serial.MessagePrefixSz+4:serial.MessagePrefixSz+8], fileID)
	return out
}

func TestDumboRootValueIsTreatedAsRootValue(t *testing.T) {
	ctx := context.Background()
	vrw := types.NewMemoryValueStore()
	ns := tree.NewTestNodeStore()

	root, err := EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)
	rtvl := root.NomsValue().(types.SerialMessage)
	require.Equal(t, serial.RootValueFileID, serial.GetFileID(rtvl))

	dmrv := withFileID(rtvl, serial.DumboRootValueFileID)
	require.Equal(t, serial.DumboRootValueFileID, serial.GetFileID(dmrv))
	require.True(t, isRootValue(vrw.Format(), dmrv))

	collectAddrs := func(msg types.SerialMessage) []hash.Hash {
		var addrs []hash.Hash
		require.NoError(t, msg.WalkAddrs(vrw.Format(), func(addr hash.Hash) error {
			addrs = append(addrs, addr)
			return nil
		}))
		return addrs
	}
	require.Equal(t, collectAddrs(rtvl), collectAddrs(dmrv))

	loaded, err := NewRootValue(ctx, vrw, ns, dmrv)
	require.NoError(t, err)
	names, err := loaded.GetTableNames(ctx, DefaultSchemaName, true)
	require.NoError(t, err)
	require.Empty(t, names)

	edited, err := loaded.CreateDatabaseSchema(ctx, schema.DatabaseSchema{Name: "s1"})
	require.NoError(t, err)
	require.Equal(t, serial.DumboRootValueFileID, serial.GetFileID(edited.NomsValue().(types.SerialMessage)))

	editedRTVL, err := root.CreateDatabaseSchema(ctx, schema.DatabaseSchema{Name: "s1"})
	require.NoError(t, err)
	require.Equal(t, serial.RootValueFileID, serial.GetFileID(editedRTVL.NomsValue().(types.SerialMessage)))
}
