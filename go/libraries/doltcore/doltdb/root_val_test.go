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

	"github.com/dolthub/dolt/go/store/types"
)

func TestForeignKeyLegacyTableNames(t *testing.T) {
	ctx := context.Background()
	fk := ForeignKey{
		Name:                   "child_parent_fk",
		TableIndex:             "parent_id",
		TableColumns:           []uint64{1},
		ReferencedTableIndex:   "id",
		ReferencedTableColumns: []uint64{2},
	}
	// Legacy foreign keys stored table names as Noms strings. Reading those
	// strings must populate the receiver before writing the new flatbuffer format.
	require.NoError(t, fk.TableName.UnmarshalNoms(ctx, types.Format_DOLT, types.String("child")))
	require.NoError(t, fk.ReferencedTableName.UnmarshalNoms(ctx, types.Format_DOLT, types.String("parent")))
	require.Equal(t, TableName{Name: "child"}, fk.TableName)
	require.Equal(t, TableName{Name: "parent"}, fk.ReferencedTableName)

	legacy, err := fk.TableName.MarshalNoms(nil)
	require.NoError(t, err)
	require.Equal(t, types.String("child"), legacy)

	collection, err := NewForeignKeyCollection(fk)
	require.NoError(t, err)
	decoded, err := deserializeFlatbufferForeignKeys(serializeFlatbufferForeignKeys(collection))
	require.NoError(t, err)
	require.Equal(t, []ForeignKey{fk}, decoded.AllKeys())
}

func TestTableNameUnmarshalNomsRejectsNonString(t *testing.T) {
	name := TableName{Name: "original"}
	err := name.UnmarshalNoms(context.Background(), types.Format_DOLT, types.Uint(1))
	require.ErrorContains(t, err, "expected only a string")
	require.Equal(t, TableName{Name: "original"}, name)
}
