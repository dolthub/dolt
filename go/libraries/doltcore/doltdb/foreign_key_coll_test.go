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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func BenchmarkForeignKeyHashOf(b *testing.B) {
	fk := ForeignKey{
		Name: "name",
		TableName: TableName{
			Name:   "tbl_name",
			Schema: "tbl_schema",
		},
		TableIndex:   "tbl_index",
		TableColumns: []uint64{1, 2, 3},
		ReferencedTableName: TableName{
			Name:   "reftbl_name",
			Schema: "reftbl_schema",
		},
		ReferencedTableIndex:   "reftbl_index",
		ReferencedTableColumns: []uint64{1, 2, 3},
		OnUpdate:               ForeignKeyReferentialAction_Cascade,
		OnDelete:               ForeignKeyReferentialAction_Cascade,
		UnresolvedFKDetails: UnresolvedFKDetails{
			TableColumns:           []string{"asdf"},
			ReferencedTableColumns: []string{"asdf"},
		},
	}
	for i := 0; i < b.N; i++ {
		_, _ = fk.HashOf()
	}
}

func TestForeignKeyEqualDefs(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*ForeignKey)
		want   bool
	}{
		{"identical", func(other *ForeignKey) {}, true},
		{"ON UPDATE differs", func(other *ForeignKey) { other.OnUpdate = ForeignKeyReferentialAction_Cascade }, false},
		{"ON DELETE differs", func(other *ForeignKey) { other.OnDelete = ForeignKeyReferentialAction_SetNull }, false},
		{"table columns differ", func(other *ForeignKey) { other.TableColumns[0] = 2 }, false},
		{"referenced table columns differ", func(other *ForeignKey) { other.ReferencedTableColumns = []uint64{1, 2} }, false},
		// Tags don't identify constraints and multiple matching definitions
		// can exist in a single table, so treat as different.
		{"name differs", func(other *ForeignKey) { other.Name = "other_name" }, false},
		// The column tag-based match handles these already so they're ignored.
		{"table names differ", func(other *ForeignKey) { other.TableName.Name = "other_name" }, true},
		{"table schema differs", func(other *ForeignKey) { other.TableName.Schema = "other_schema" }, true},
		{"table index differs", func(other *ForeignKey) { other.TableIndex = "other_index" }, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fk := newForeignKey()
			other := newForeignKey()
			test.modify(&other)
			assert.Equal(t, test.want, fk.EqualDefs(other))
			assert.Equal(t, test.want, other.EqualDefs(fk))
		})
	}
}

// TestKeysForTable verifies that KeysForTable matches tables case-insensitively, exactly as TableName.EqualFold does,
// and stays correct as keys are removed and the collection is copied.
func TestKeysForTable(t *testing.T) {
	tables := []TableName{
		{Name: "child"},
		{Name: "CHILD"},
		{Name: "Child", Schema: "public"},
		{Name: "child", Schema: "PUBLIC"},
		{Name: "other"},
		// The two lower case forms of sigma fold together, although strings.ToLower keeps them apart.
		{Name: "σ"},
		{Name: "ς"},
		// The dotted capital I lowers to "i" but does not fold to it.
		{Name: "İ"},
		{Name: "i"},
	}
	fkc, err := NewForeignKeyCollection()
	assert.NoError(t, err)
	var added []ForeignKey
	for i, tableName := range tables {
		// Two keys per table, added in descending name order, so the ordering of the result is exercised too.
		for _, prefix := range []string{"b", "a"} {
			fk := newForeignKey()
			fk.Name = fmt.Sprintf("%s_fk_%d", prefix, i)
			fk.TableName = tableName
			fk.TableColumns = []uint64{uint64(100*i) + uint64(prefix[0])}
			fk.ReferencedTableName = tables[(i+1)%len(tables)]
			assert.NoError(t, fkc.AddKeys(fk))
			added = append(added, fk)
		}
	}

	lookups := append([]TableName{
		{Name: "cHiLd"},
		{Name: "child", Schema: "Public"},
		{Name: "OTHER"},
		{Name: "Σ"},
		{Name: "I"},
		{Name: "missing"},
	}, tables...)
	requireMatchesScan := func(fkc *ForeignKeyCollection) {
		for _, tableName := range lookups {
			var expectedDeclared, expectedReferenced []ForeignKey
			for _, fk := range fkc.AllKeys() {
				if fk.TableName.EqualFold(tableName) {
					expectedDeclared = append(expectedDeclared, fk)
				}
				if fk.ReferencedTableName.EqualFold(tableName) {
					expectedReferenced = append(expectedReferenced, fk)
				}
			}
			declared, referenced := fkc.KeysForTable(tableName)
			assert.Equal(t, expectedDeclared, declared, "table %q in schema %q", tableName.Name, tableName.Schema)
			assert.Equal(t, expectedReferenced, referenced, "table %q in schema %q", tableName.Name, tableName.Schema)
		}
	}
	requireMatchesScan(fkc)

	copied := fkc.Copy()
	copied.RemoveKeys(added[0], added[5])
	assert.True(t, copied.RemoveKeyByName(added[8].Name, added[8].TableName))
	requireMatchesScan(copied)
	requireMatchesScan(fkc)
	assert.Equal(t, len(added), fkc.Count())
	assert.Equal(t, len(added)-3, copied.Count())

	// The indexes of |fkc| were built by the lookups above, and must be kept up to date as keys are removed and added.
	assert.True(t, fkc.RemoveKeyByName(added[3].Name, added[3].TableName))
	readded := added[3]
	readded.ReferencedTableName = tables[0]
	assert.NoError(t, fkc.AddKeys(readded))
	requireMatchesScan(fkc)
}

// TestForeignKeyCollectionNameIndex verifies that name lookups stay correct as keys are added, removed, copied and
// loaded from storage.
func TestForeignKeyCollectionNameIndex(t *testing.T) {
	newKey := func(name string, table TableName, tag uint64) ForeignKey {
		fk := newForeignKey()
		fk.Name = name
		fk.TableName = table
		fk.TableColumns = []uint64{tag}
		return fk
	}
	child := TableName{Name: "child", Schema: "public"}
	other := TableName{Name: "other", Schema: "public"}
	onChild := newKey("fk_a", child, 1)
	onOther := newKey("FK_A", other, 2)
	sigma := newKey("fk_σ", child, 3)

	fkc, err := NewForeignKeyCollection(onChild, onOther, sigma)
	assert.NoError(t, err)

	lookup := func(fkc *ForeignKeyCollection, name string, table TableName) (ForeignKey, bool) {
		return fkc.GetByNameCaseInsensitive(name, table)
	}
	fk, ok := lookup(fkc, "Fk_A", TableName{Name: "CHILD", Schema: "PUBLIC"})
	assert.True(t, ok)
	assert.Equal(t, onChild, fk)
	fk, ok = lookup(fkc, "fk_a", other)
	assert.True(t, ok)
	assert.Equal(t, onOther, fk)
	// The two lower case forms of sigma fold together.
	fk, ok = lookup(fkc, "FK_ς", child)
	assert.True(t, ok)
	assert.Equal(t, sigma, fk)
	// With a schema, the key must be declared on the given table. Without one, a key on any table matches.
	_, ok = lookup(fkc, "fk_a", TableName{Name: "unrelated", Schema: "public"})
	assert.False(t, ok)
	_, ok = lookup(fkc, "fk_a", TableName{Name: "unrelated"})
	assert.True(t, ok)

	err = fkc.AddKeys(newKey("FK_Σ", child, 4))
	assert.True(t, sql.ErrForeignKeyDuplicateName.Is(err), "unexpected error: %v", err)

	copied := fkc.Copy()
	assert.True(t, copied.RemoveKeyByName("fk_A", child))
	_, ok = lookup(copied, "fk_a", child)
	assert.False(t, ok)
	_, ok = lookup(copied, "fk_a", other)
	assert.True(t, ok)
	_, ok = lookup(fkc, "fk_a", child)
	assert.True(t, ok, "removing a key from a copy must not remove it from the original")

	assert.NoError(t, copied.RemoveTables(context.Background(), other))
	_, ok = lookup(copied, "fk_a", other)
	assert.False(t, ok)
	assert.NoError(t, copied.AddKeys(newKey("fk_a", other, 5)))

	// Without a schema, as in Dolt, a foreign key name is unique across all tables.
	noSchema := newKey("fk_b", TableName{Name: "child"}, 6)
	assert.NoError(t, fkc.AddKeys(noSchema))
	fk, ok = lookup(fkc, "FK_B", TableName{Name: "unrelated"})
	assert.True(t, ok)
	assert.Equal(t, noSchema, fk)
	err = fkc.AddKeys(newKey("FK_B", TableName{Name: "other"}, 7))
	assert.True(t, sql.ErrForeignKeyDuplicateName.Is(err), "unexpected error: %v", err)

	loaded, err := deserializeFlatbufferForeignKeys(serializeFlatbufferForeignKeys(fkc))
	assert.NoError(t, err)
	for _, expected := range []ForeignKey{onChild, onOther, sigma, noSchema} {
		fk, ok = lookup(loaded, expected.Name, expected.TableName)
		assert.True(t, ok)
		assert.Equal(t, expected, fk)
	}
}

// TestGetMatchingKey verifies that GetMatchingKey matches resolved keys by column tags, also across a table rename, and
// unresolved keys by table and column names.
func TestGetMatchingKey(t *testing.T) {
	resolved := newForeignKey()
	unresolved := ForeignKey{
		Name:                "fk_unresolved",
		TableName:           TableName{Name: "child"},
		ReferencedTableName: TableName{Name: "parent"},
		UnresolvedFKDetails: UnresolvedFKDetails{
			TableColumns:           []string{"c3"},
			ReferencedTableColumns: []string{"p4"},
		},
	}
	fkc, err := NewForeignKeyCollection(resolved, unresolved)
	assert.NoError(t, err)

	renamed := resolved
	renamed.TableName = TableName{Name: "renamed_child"}
	fk, ok := fkc.GetMatchingKey(renamed, nil, false)
	assert.True(t, ok)
	assert.Equal(t, resolved, fk)

	otherTags := resolved
	otherTags.TableColumns = []uint64{5}
	_, ok = fkc.GetMatchingKey(otherTags, nil, false)
	assert.False(t, ok)

	sameUnresolved := unresolved
	sameUnresolved.Name = "other_name"
	fk, ok = fkc.GetMatchingKey(sameUnresolved, nil, false)
	assert.True(t, ok)
	assert.Equal(t, unresolved, fk)

	otherTable := unresolved
	otherTable.TableName = TableName{Name: "other"}
	_, ok = fkc.GetMatchingKey(otherTable, nil, false)
	assert.False(t, ok)

	// A resolved key matches an unresolved one when the schemas map its column names to the same tags.
	schemas := map[TableName]schema.Schema{
		{Name: "child"}:  schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("c3", 3, types.IntKind, true))),
		{Name: "parent"}: schema.MustSchemaFromCols(schema.NewColCollection(schema.NewColumn("p4", 4, types.IntKind, true))),
	}
	resolvedVersion := unresolved
	resolvedVersion.TableColumns = []uint64{3}
	resolvedVersion.ReferencedTableColumns = []uint64{4}
	fk, ok = fkc.GetMatchingKey(resolvedVersion, schemas, false)
	assert.True(t, ok)
	assert.Equal(t, unresolved, fk)
}

// BenchmarkDeserializeForeignKeys loads a collection of 400 foreign keys, which every new root value does on its
// first use of the collection.
func BenchmarkDeserializeForeignKeys(b *testing.B) {
	msg := serializeFlatbufferForeignKeys(benchmarkForeignKeyCollection(b))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := deserializeFlatbufferForeignKeys(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCopyForeignKeys copies a collection of 400 foreign keys, which every call to
// RootValue.GetForeignKeyCollection does.
func BenchmarkCopyForeignKeys(b *testing.B) {
	fkc := benchmarkForeignKeyCollection(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fkc.Copy()
	}
}

// benchmarkForeignKeyCollection returns a collection of 400 foreign keys declared on 200 tables.
func benchmarkForeignKeyCollection(b *testing.B) *ForeignKeyCollection {
	fkc, err := NewForeignKeyCollection()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		fk := newForeignKey()
		fk.Name = fmt.Sprintf("fk_%04d", i)
		fk.TableName = TableName{Name: fmt.Sprintf("t%04d", i%200), Schema: "public"}
		fk.TableColumns = []uint64{uint64(i)}
		if err = fkc.AddKeys(fk); err != nil {
			b.Fatal(err)
		}
	}
	return fkc
}

// newForeignKey returns a pre-defined foreign key with no action for testing.
func newForeignKey() ForeignKey {
	return ForeignKey{
		Name:                   "fk1",
		TableName:              TableName{Name: "child"},
		TableIndex:             "child_idx",
		TableColumns:           []uint64{1},
		ReferencedTableName:    TableName{Name: "parent"},
		ReferencedTableIndex:   "parent_idx",
		ReferencedTableColumns: []uint64{2},
		OnUpdate:               ForeignKeyReferentialAction_NoAction,
		OnDelete:               ForeignKeyReferentialAction_NoAction,
	}
}
