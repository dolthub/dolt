// Copyright 2025 Dolthub, Inc.
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

package commands

import (
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

func TestDiffTypeFilter_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		filterBy string
		want     bool
	}{
		{"valid: added", diff.DiffTypeAdded, true},
		{"valid: modified", diff.DiffTypeModified, true},
		{"valid: removed", diff.DiffTypeRemoved, true},
		{"valid: all", diff.DiffTypeAll, true},
		{"invalid: empty string with nil filter", "", true}, // nil filter is valid
		{"invalid: random string", "invalid", false},
		{"invalid: uppercase", "ADDED", false},
		{"invalid: typo addedd", "addedd", false},
		{"invalid: plural adds", "adds", false},
		{"invalid: typo modifiedd", "modifiedd", false},
		{"invalid: typo removedd", "removedd", false},
		{"invalid: insert instead of added", "insert", false},
		{"invalid: update instead of modified", "update", false},
		{"invalid: delete instead of removed", "delete", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := newDiffTypeFilter(tt.filterBy)
			got := df.isValid()
			if got != tt.want {
				t.Errorf("isValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_ShouldInclude(t *testing.T) {
	tests := []struct {
		name       string
		filterType string
		checkType  string
		want       bool
	}{
		// Testing with filter=added
		{"filter=added, check added", diff.DiffTypeAdded, diff.DiffTypeAdded, true},
		{"filter=added, check modified", diff.DiffTypeAdded, diff.DiffTypeModified, false},
		{"filter=added, check removed", diff.DiffTypeAdded, diff.DiffTypeRemoved, false},

		// Testing with filter=modified
		{"filter=modified, check added", diff.DiffTypeModified, diff.DiffTypeAdded, false},
		{"filter=modified, check modified", diff.DiffTypeModified, diff.DiffTypeModified, true},
		{"filter=modified, check removed", diff.DiffTypeModified, diff.DiffTypeRemoved, false},

		// Testing with filter=removed
		{"filter=removed, check added", diff.DiffTypeRemoved, diff.DiffTypeAdded, false},
		{"filter=removed, check modified", diff.DiffTypeRemoved, diff.DiffTypeModified, false},
		{"filter=removed, check removed", diff.DiffTypeRemoved, diff.DiffTypeRemoved, true},

		// Testing with filter=all
		{"filter=all, check added", diff.DiffTypeAll, diff.DiffTypeAdded, true},
		{"filter=all, check modified", diff.DiffTypeAll, diff.DiffTypeModified, true},
		{"filter=all, check removed", diff.DiffTypeAll, diff.DiffTypeRemoved, true},

		// Testing with empty filter (nil filters map)
		{"filter=empty, check added", "", diff.DiffTypeAdded, true},
		{"filter=empty, check modified", "", diff.DiffTypeModified, true},
		{"filter=empty, check removed", "", diff.DiffTypeRemoved, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := newDiffTypeFilter(tt.filterType)
			got := df.shouldInclude(tt.checkType)
			if got != tt.want {
				t.Errorf("shouldInclude(%s) = %v, want %v", tt.checkType, got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_ConsistencyAcrossMethods(t *testing.T) {
	// Test that filter=all returns true for all diff types
	t.Run("filter=all returns true for all types", func(t *testing.T) {
		df := newDiffTypeFilter(diff.DiffTypeAll)

		if !df.shouldInclude(diff.DiffTypeAdded) {
			t.Error("filter=all should include added")
		}
		if !df.shouldInclude(diff.DiffTypeRemoved) {
			t.Error("filter=all should include removed")
		}
		if !df.shouldInclude(diff.DiffTypeModified) {
			t.Error("filter=all should include modified")
		}
	})

	// Test that each specific filter only returns true for its type
	t.Run("filter=added only includes added", func(t *testing.T) {
		df := newDiffTypeFilter(diff.DiffTypeAdded)

		if !df.shouldInclude(diff.DiffTypeAdded) {
			t.Error("filter=added should include added")
		}
		if df.shouldInclude(diff.DiffTypeRemoved) {
			t.Error("filter=added should not include removed")
		}
		if df.shouldInclude(diff.DiffTypeModified) {
			t.Error("filter=added should not include modified")
		}
	})

	t.Run("filter=removed only includes removed", func(t *testing.T) {
		df := newDiffTypeFilter(diff.DiffTypeRemoved)

		if df.shouldInclude(diff.DiffTypeAdded) {
			t.Error("filter=removed should not include added")
		}
		if !df.shouldInclude(diff.DiffTypeRemoved) {
			t.Error("filter=removed should include removed")
		}
		if df.shouldInclude(diff.DiffTypeModified) {
			t.Error("filter=removed should not include modified")
		}
	})

	t.Run("filter=modified only includes modified", func(t *testing.T) {
		df := newDiffTypeFilter(diff.DiffTypeModified)

		if df.shouldInclude(diff.DiffTypeAdded) {
			t.Error("filter=modified should not include added")
		}
		if df.shouldInclude(diff.DiffTypeRemoved) {
			t.Error("filter=modified should not include removed")
		}
		if !df.shouldInclude(diff.DiffTypeModified) {
			t.Error("filter=modified should include modified")
		}
	})
}

func TestDiffTypeFilter_InvalidFilterBehavior(t *testing.T) {
	// Test that invalid filters return false for isValid
	invalidFilters := []string{"invalid", "ADDED", "addedd", "delete"}

	for _, filterValue := range invalidFilters {
		t.Run("invalid filter: "+filterValue, func(t *testing.T) {
			df := newDiffTypeFilter(filterValue)

			if df.isValid() {
				t.Errorf("Filter %s should be invalid", filterValue)
			}
		})
	}
}

func TestFilterConstants(t *testing.T) {
	// Test that filter constants have expected values
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"DiffTypeAdded value", diff.DiffTypeAdded, "added"},
		{"DiffTypeModified value", diff.DiffTypeModified, "modified"},
		{"DiffTypeRemoved value", diff.DiffTypeRemoved, "removed"},
		{"DiffTypeAll value", diff.DiffTypeAll, "all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Expected %s = %s, got %s", tt.name, tt.expected, tt.constant)
			}
		})
	}
}

func TestFilterConstants_AreUnique(t *testing.T) {
	// Test that all filter constants are unique
	constants := []string{diff.DiffTypeAdded, diff.DiffTypeModified, diff.DiffTypeRemoved, diff.DiffTypeAll}
	seen := make(map[string]bool)

	for _, c := range constants {
		if seen[c] {
			t.Errorf("Duplicate filter constant value: %s", c)
		}
		seen[c] = true
	}

	if len(seen) != 4 {
		t.Errorf("Expected 4 unique filter constants, got %d", len(seen))
	}
}

func TestFilterConstants_AreLowercase(t *testing.T) {
	// Test that filter constants are lowercase (convention)
	constants := []string{diff.DiffTypeAdded, diff.DiffTypeModified, diff.DiffTypeRemoved, diff.DiffTypeAll}

	for _, c := range constants {
		if c != strings.ToLower(c) {
			t.Errorf("Filter constant %s should be lowercase", c)
		}
	}
}

func TestShouldUseLazyHeader(t *testing.T) {
	tests := []struct {
		name           string
		filterType     string
		schemaChange   bool
		isRename       bool
		expectedResult bool
	}{
		{
			name:           "use lazy: filter active, data-only change",
			filterType:     diff.DiffTypeAdded,
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
		{
			name:           "don't use lazy: no filter",
			filterType:     "",
			schemaChange:   false,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: filter is all",
			filterType:     diff.DiffTypeAll,
			schemaChange:   false,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: schema changed",
			filterType:     diff.DiffTypeModified,
			schemaChange:   true,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: table renamed",
			filterType:     diff.DiffTypeRemoved,
			schemaChange:   false,
			isRename:       true,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: schema changed AND renamed",
			filterType:     diff.DiffTypeAdded,
			schemaChange:   true,
			isRename:       true,
			expectedResult: false,
		},
		{
			name:           "use lazy: filter=modified, data-only",
			filterType:     diff.DiffTypeModified,
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
		{
			name:           "use lazy: filter=removed, data-only",
			filterType:     diff.DiffTypeRemoved,
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filter *diffTypeFilter
			if tt.filterType != "" {
				filter = newDiffTypeFilter(tt.filterType)
			}

			dArgs := &diffArgs{
				diffDisplaySettings: &diffDisplaySettings{
					filter: filter,
				},
			}
			tableSummary := diff.TableDeltaSummary{
				SchemaChange: tt.schemaChange,
			}
			// Create a mock rename by setting different from/to names
			if tt.isRename {
				tableSummary.FromTableName = doltdb.TableName{Name: "old_table"}
				tableSummary.ToTableName = doltdb.TableName{Name: "new_table"}
			} else {
				tableSummary.FromTableName = doltdb.TableName{Name: "table"}
				tableSummary.ToTableName = doltdb.TableName{Name: "table"}
			}

			result := shouldUseLazyHeader(dArgs, tableSummary)

			if result != tt.expectedResult {
				t.Errorf("%s: expected %v, got %v", tt.name, tt.expectedResult, result)
			}
		})
	}
}

// mockDiffWriter is a test implementation of diffWriter
type mockDiffWriter struct {
	beginTableCalled bool
	beginTableError  error
}

func (m *mockDiffWriter) BeginTable(_ /* fromTableName */, _ /* toTableName */ string, _ /* isAdd */, _ /* isDrop */ bool) error {
	m.beginTableCalled = true
	return m.beginTableError
}

func (m *mockDiffWriter) WriteTableSchemaDiff(_ /* fromTableInfo */, _ /* toTableInfo */ *diff.TableInfo, _ /* tds */ diff.TableDeltaSummary) error {
	return nil
}

func (m *mockDiffWriter) WriteEventDiff(_ /* ctx */ context.Context, _ /* eventName */, _ /* oldDefn */, _ /* newDefn */ string) error {
	return nil
}

func (m *mockDiffWriter) WriteTriggerDiff(_ /* ctx */ context.Context, _ /* triggerName */, _ /* oldDefn */, _ /* newDefn */ string) error {
	return nil
}

func (m *mockDiffWriter) WriteViewDiff(_ /* ctx */ context.Context, _ /* viewName */, _ /* oldDefn */, _ /* newDefn */ string) error {
	return nil
}

func (m *mockDiffWriter) WriteTableDiffStats(_ /* diffStats */ []diffStatistics, _ /* oldColLen */, _ /* newColLen */ int, _ /* areTablesKeyless */ bool) error {
	return nil
}

func (m *mockDiffWriter) RowWriter(_ /* fromTableInfo */, _ /* toTableInfo */ *diff.TableInfo, _ /* tds */ diff.TableDeltaSummary, _ /* unionSch */ sql.Schema) (diff.SqlRowDiffWriter, error) {
	return &mockRowWriter{}, nil
}

func (m *mockDiffWriter) Close(_ /* ctx */ context.Context) error {
	return nil
}

// mockRowWriter is a test implementation of SqlRowDiffWriter
type mockRowWriter struct {
	writeCalled bool
	closeCalled bool
}

func (m *mockRowWriter) WriteRow(_ /* ctx */ *sql.Context, _ /* row */ sql.Row, _ /* diffType */ diff.ChangeType, _ /* colDiffTypes */ []diff.ChangeType) error {
	m.writeCalled = true
	return nil
}

func (m *mockRowWriter) WriteCombinedRow(_ /* ctx */ *sql.Context, _ /* oldRow */, _ /* newRow */ sql.Row, _ /* mode */ diff.Mode) error {
	m.writeCalled = true
	return nil
}

func (m *mockRowWriter) Close(_ /* ctx */ context.Context) error {
	m.closeCalled = true
	return nil
}

func TestLazyRowWriter_NoRowsWritten(t *testing.T) {
	mockDW := &mockDiffWriter{}
	realWriter := &mockRowWriter{}

	beginTableCalled := false
	onFirstWrite := func() error {
		beginTableCalled = true
		return mockDW.BeginTable("fromTable", "toTable", false, false)
	}

	lazyWriter := newLazyRowWriter(realWriter, onFirstWrite)

	// Close without writing any rows
	err := lazyWriter.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// BeginTable should NEVER have been called
	if beginTableCalled {
		t.Error("BeginTable() was called even though no rows were written - should have been lazy!")
	}
}

func TestLazyRowWriter_RowsWritten(t *testing.T) {
	mockDW := &mockDiffWriter{}
	realWriter := &mockRowWriter{}

	onFirstWrite := func() error {
		return mockDW.BeginTable("fromTable", "toTable", false, false)
	}

	lazyWriter := newLazyRowWriter(realWriter, onFirstWrite)

	// Write a row
	ctx := sql.NewEmptyContext()
	err := lazyWriter.WriteRow(ctx, sql.Row{}, diff.Added, []diff.ChangeType{})
	if err != nil {
		t.Fatalf("WriteRow() returned error: %v", err)
	}

	// BeginTable should have been called on first write
	if !mockDW.beginTableCalled {
		t.Error("BeginTable() was NOT called after writing a row - should have been initialized!")
	}

	// Close
	err = lazyWriter.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestLazyRowWriter_CombinedRowsWritten(t *testing.T) {
	mockDW := &mockDiffWriter{}
	realWriter := &mockRowWriter{}

	onFirstWrite := func() error {
		return mockDW.BeginTable("fromTable", "toTable", false, false)
	}

	lazyWriter := newLazyRowWriter(realWriter, onFirstWrite)

	// Write a combined row
	ctx := sql.NewEmptyContext()
	err := lazyWriter.WriteCombinedRow(ctx, sql.Row{}, sql.Row{}, diff.ModeRow)
	if err != nil {
		t.Fatalf("WriteCombinedRow() returned error: %v", err)
	}

	// BeginTable should have been called on first write
	if !mockDW.beginTableCalled {
		t.Error("BeginTable() was NOT called after writing combined row - should have been initialized!")
	}
}

func TestLazyRowWriter_InitializedOnlyOnce(t *testing.T) {
	callCount := 0
	mockDW := &mockDiffWriter{}
	realWriter := &mockRowWriter{}

	onFirstWrite := func() error {
		callCount++
		return mockDW.BeginTable("fromTable", "toTable", false, false)
	}

	lazyWriter := newLazyRowWriter(realWriter, onFirstWrite)

	ctx := sql.NewEmptyContext()

	// Write multiple rows
	for i := 0; i < 5; i++ {
		err := lazyWriter.WriteRow(ctx, sql.Row{}, diff.Added, []diff.ChangeType{})
		if err != nil {
			t.Fatalf("WriteRow() %d returned error: %v", i, err)
		}
	}

	// BeginTable should have been called exactly ONCE (on first write only)
	if callCount != 1 {
		t.Errorf("BeginTable() called %d times, expected exactly 1", callCount)
	}
}

func TestShouldSkipRow(t *testing.T) {
	tests := []struct {
		name           string
		filterType     string
		rowChangeType  diff.ChangeType
		expectedResult bool
	}{
		{"filter=added, row=Added", diff.DiffTypeAdded, diff.Added, false},
		{"filter=added, row=Removed", diff.DiffTypeAdded, diff.Removed, true},
		{"filter=added, row=ModifiedOld", diff.DiffTypeAdded, diff.ModifiedOld, true},
		{"filter=added, row=ModifiedNew", diff.DiffTypeAdded, diff.ModifiedNew, true},

		{"filter=removed, row=Added", diff.DiffTypeRemoved, diff.Added, true},
		{"filter=removed, row=Removed", diff.DiffTypeRemoved, diff.Removed, false},
		{"filter=removed, row=ModifiedOld", diff.DiffTypeRemoved, diff.ModifiedOld, true},

		{"filter=modified, row=Added", diff.DiffTypeModified, diff.Added, true},
		{"filter=modified, row=Removed", diff.DiffTypeModified, diff.Removed, true},
		{"filter=modified, row=ModifiedOld", diff.DiffTypeModified, diff.ModifiedOld, false},
		{"filter=modified, row=ModifiedNew", diff.DiffTypeModified, diff.ModifiedNew, false},

		{"filter=all, row=Added", diff.DiffTypeAll, diff.Added, false},
		{"filter=all, row=Removed", diff.DiffTypeAll, diff.Removed, false},
		{"filter=all, row=ModifiedOld", diff.DiffTypeAll, diff.ModifiedOld, false},

		{"nil filter, row=Added", "", diff.Added, false},
		{"nil filter, row=Removed", "", diff.Removed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filter *diffTypeFilter
			if tt.filterType != "" {
				filter = newDiffTypeFilter(tt.filterType)
			}

			result := shouldSkipRow(filter, tt.rowChangeType)

			if result != tt.expectedResult {
				t.Errorf("expected %v, got %v", tt.expectedResult, result)
			}
		})
	}
}
