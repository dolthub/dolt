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
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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

// TestGetForeignKeysForTable verifies that GetForeignKeysForTable matches KeysForTable on a copy of the root's
// collection, is unaffected by changes to such copies, is safe for concurrent use, and doesn't cost more as the root
// gains foreign keys on unrelated tables.
func TestGetForeignKeysForTable(t *testing.T) {
	ctx := context.Background()
	ddb, err := LoadDoltDB(ctx, types.Format_DOLT, InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer ddb.Close()

	empty, err := EmptyRootValue(ctx, ddb.ValueReadWriter(), ddb.NodeStore())
	require.NoError(t, err)
	declared, referencedBy, err := empty.GetForeignKeysForTable(ctx, TableName{Name: "parent"})
	require.NoError(t, err)
	require.Empty(t, declared)
	require.Empty(t, referencedBy)

	// 400 tables each declare a key referencing |parent|, which declares a key of its own.
	parent := TableName{Name: "parent"}
	fkc, err := NewForeignKeyCollection()
	require.NoError(t, err)
	for i := 0; i < 400; i++ {
		fk := newForeignKey()
		fk.Name = fmt.Sprintf("fk_%04d", i)
		fk.TableName = TableName{Name: fmt.Sprintf("child_%04d", i)}
		fk.TableColumns = []uint64{uint64(i)}
		fk.ReferencedTableName = parent
		require.NoError(t, fkc.AddKeys(fk))
	}
	parentFk := newForeignKey()
	parentFk.Name = "parent_fk"
	parentFk.TableName = parent
	parentFk.ReferencedTableName = TableName{Name: "grandparent"}
	require.NoError(t, fkc.AddKeys(parentFk))
	root, err := empty.PutForeignKeyCollection(ctx, fkc)
	require.NoError(t, err)

	lookups := []TableName{parent, {Name: "PARENT"}, {Name: "child_0007"}, {Name: "grandparent"}, {Name: "missing"}}
	requireMatchesCollection := func(root RootValue) {
		copied, err := root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		for _, tableName := range lookups {
			expectedDeclared, expectedReferencedBy := copied.KeysForTable(tableName)
			declared, referencedBy, err := root.GetForeignKeysForTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, expectedDeclared, declared, "table %s", tableName)
			require.Equal(t, expectedReferencedBy, referencedBy, "table %s", tableName)
		}
	}

	// Sessions sharing a root may load its collection, and look keys up in it, concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, referencedBy, err := root.GetForeignKeysForTable(ctx, parent)
			require.NoError(t, err)
			require.Len(t, referencedBy, 400)
		}()
	}
	wg.Wait()
	requireMatchesCollection(root)

	// Changing a collection returned by GetForeignKeyCollection must not change the keys this root returns.
	copied, err := root.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, copied.RemoveTables(ctx, TableName{Name: "child_0007"}))
	declared, _, err = root.GetForeignKeysForTable(ctx, TableName{Name: "child_0007"})
	require.NoError(t, err)
	require.Len(t, declared, 1)
	requireMatchesCollection(root)

	// Planning a write to a table looks up the keys of every table that references it, so a lookup must not cost more
	// as the root gains keys on other tables. Copying this root's collection alone takes hundreds of allocations.
	allocs := testing.AllocsPerRun(10, func() {
		_, _, err := root.GetForeignKeysForTable(ctx, TableName{Name: "child_0007"})
		require.NoError(t, err)
	})
	require.LessOrEqual(t, allocs, 10.0)
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
