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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
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

// TestDeclaredKeysByTable verifies that the grouping returned by DeclaredKeysByTable gives, for every table name, the
// same foreign keys in the same order as the declaredFk result of KeysForTable.
func TestDeclaredKeysByTable(t *testing.T) {
	declaringTables := []TableName{
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
	for i, tableName := range declaringTables {
		// Two keys per table, added in descending name order, so the ordering of the result is exercised too.
		for _, prefix := range []string{"b", "a"} {
			fk := newForeignKey()
			fk.Name = fmt.Sprintf("%s_fk_%d", prefix, i)
			fk.TableName = tableName
			fk.TableColumns = []uint64{uint64(100*i) + uint64(prefix[0])}
			assert.NoError(t, fkc.AddKeys(fk))
		}
	}

	lookups := append([]TableName{
		{Name: "cHiLd"},
		{Name: "child", Schema: "Public"},
		{Name: "OTHER"},
		{Name: "Σ"},
		{Name: "I"},
		{Name: "parent"},
		{Name: "missing"},
	}, declaringTables...)
	byTable := fkc.DeclaredKeysByTable()
	for _, tableName := range lookups {
		expected, _ := fkc.KeysForTable(tableName)
		assert.Equal(t, expected, byTable.KeysForTable(tableName), "table %q in schema %q", tableName.Name, tableName.Schema)
	}
}

// TestForeignKeyNames verifies that foreignKeyNames reports a duplicate name for exactly the keys for which
// GetByNameCaseInsensitive finds a key in the collection.
func TestForeignKeyNames(t *testing.T) {
	keys := []struct {
		name  string
		table TableName
	}{
		{"fk", TableName{Name: "child"}},
		{"FK", TableName{Name: "other"}},
		{"fk", TableName{Name: "child", Schema: "public"}},
		{"Fk", TableName{Name: "CHILD", Schema: "PUBLIC"}},
		{"fk", TableName{Name: "child", Schema: "other"}},
		{"fk", TableName{Name: "other", Schema: "public"}},
		{"fk2", TableName{Name: "child", Schema: "public"}},
		{"FK2", TableName{Name: "child"}},
		{"fk3", TableName{Name: "child"}},
		{"Fk3", TableName{Name: "child", Schema: "public"}},
		{"", TableName{Name: "child"}},
		{"", TableName{Name: "child", Schema: "public"}},
		// The two lower case forms of sigma fold together.
		{"fk_σ", TableName{Name: "child"}},
		{"fk_ς", TableName{Name: "child"}},
	}
	fkc, err := NewForeignKeyCollection()
	assert.NoError(t, err)
	names := newForeignKeyNames(len(keys))
	for i, k := range keys {
		fk := newForeignKey()
		fk.Name = k.name
		fk.TableName = k.table
		fk.TableColumns = []uint64{uint64(i)}

		_, expected := fkc.GetByNameCaseInsensitive(fk.Name, fk.TableName)
		assert.Equal(t, expected, names.contains(fk), "key %d: %q on table %q in schema %q", i, k.name, k.table.Name, k.table.Schema)

		// Every key goes into the collection, also the ones AddKeys would reject, so that later keys are compared
		// with all earlier ones.
		assert.NoError(t, fkc.addKeyWithUniqueName(fk))
		names.add(fk)
	}
}

// TestDeserializeForeignKeysRejectsDuplicateNames verifies that loading a stored collection still fails when two of
// its keys have the same name.
func TestDeserializeForeignKeysRejectsDuplicateNames(t *testing.T) {
	first := newForeignKey()
	second := newForeignKey()
	second.Name = "FK1"
	second.TableColumns = []uint64{3}

	fkc, err := NewForeignKeyCollection()
	assert.NoError(t, err)
	for _, fk := range []ForeignKey{first, second} {
		fkHash, err := fk.HashOf()
		assert.NoError(t, err)
		fkc.foreignKeys[fkHash.String()] = fk
	}

	_, err = deserializeFlatbufferForeignKeys(serializeFlatbufferForeignKeys(fkc))
	assert.True(t, sql.ErrForeignKeyDuplicateName.Is(err), "unexpected error: %v", err)

	// Without the second key the collection loads and holds the first one.
	fkc, err = NewForeignKeyCollection(first)
	assert.NoError(t, err)
	loaded, err := deserializeFlatbufferForeignKeys(serializeFlatbufferForeignKeys(fkc))
	assert.NoError(t, err)
	assert.Equal(t, []ForeignKey{first}, loaded.AllKeys())
}

// BenchmarkDeserializeForeignKeys loads a collection of 400 foreign keys, which every new root value does on its
// first use of the collection.
func BenchmarkDeserializeForeignKeys(b *testing.B) {
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
	msg := serializeFlatbufferForeignKeys(fkc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = deserializeFlatbufferForeignKeys(msg); err != nil {
			b.Fatal(err)
		}
	}
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
