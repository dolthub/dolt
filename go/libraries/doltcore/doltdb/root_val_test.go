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
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

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

// TestFoldString verifies that foldString maps two strings to the same value exactly when strings.EqualFold treats them
// as equal: every rune that a rune folds to has the same representative, and that representative is one of them.
func TestFoldString(t *testing.T) {
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		folded := foldString(string(r))
		require.True(t, strings.EqualFold(string(r), folded), "rune %U folds to %q", r, folded)
		for c := unicode.SimpleFold(r); c != r; c = unicode.SimpleFold(c) {
			require.Equal(t, folded, foldString(string(c)), "runes %U and %U", r, c)
		}
	}

	require.Equal(t, "child_1", foldString("child_1"))
	require.Equal(t, "child_1", foldString("CHILD_1"))
	require.Equal(t, foldString("σ"), foldString("ς"))
	require.NotEqual(t, foldString("İ"), foldString("i"))
}
