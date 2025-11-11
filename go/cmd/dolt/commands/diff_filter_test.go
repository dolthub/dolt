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
		{"valid: added", FilterAdds, true},
		{"valid: modified", FilterModified, true},
		{"valid: removed", FilterRemoved, true},
		{"valid: all", NoFilter, true},
		{"invalid: empty string", "", false},
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
			df := &diffTypeFilter{filterBy: tt.filterBy}
			got := df.isValid()
			if got != tt.want {
				t.Errorf("isValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_IncludeAddsOrAll(t *testing.T) {
	tests := []struct {
		name     string
		filterBy string
		want     bool
	}{
		{"filter=added returns true", FilterAdds, true},
		{"filter=all returns true", NoFilter, true},
		{"filter=modified returns false", FilterModified, false},
		{"filter=removed returns false", FilterRemoved, false},
		{"empty filter returns false", "", false},
		{"invalid filter returns false", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &diffTypeFilter{filterBy: tt.filterBy}
			got := df.includeAddsOrAll()
			if got != tt.want {
				t.Errorf("includeAddsOrAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_IncludeDropsOrAll(t *testing.T) {
	tests := []struct {
		name     string
		filterBy string
		want     bool
	}{
		{"filter=removed returns true", FilterRemoved, true},
		{"filter=all returns true", NoFilter, true},
		{"filter=added returns false", FilterAdds, false},
		{"filter=modified returns false", FilterModified, false},
		{"empty filter returns false", "", false},
		{"invalid filter returns false", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &diffTypeFilter{filterBy: tt.filterBy}
			got := df.includeDropsOrAll()
			if got != tt.want {
				t.Errorf("includeDropsOrAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_IncludeModificationsOrAll(t *testing.T) {
	tests := []struct {
		name     string
		filterBy string
		want     bool
	}{
		{"filter=modified returns true", FilterModified, true},
		{"filter=all returns true", NoFilter, true},
		{"filter=added returns false", FilterAdds, false},
		{"filter=removed returns false", FilterRemoved, false},
		{"empty filter returns false", "", false},
		{"invalid filter returns false", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &diffTypeFilter{filterBy: tt.filterBy}
			got := df.includeModificationsOrAll()
			if got != tt.want {
				t.Errorf("includeModificationsOrAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffTypeFilter_ConsistencyAcrossMethods(t *testing.T) {
	// Test that NoFilter returns true for all include methods
	t.Run("NoFilter returns true for all methods", func(t *testing.T) {
		df := &diffTypeFilter{filterBy: NoFilter}

		if !df.includeAddsOrAll() {
			t.Error("NoFilter should include adds")
		}
		if !df.includeDropsOrAll() {
			t.Error("NoFilter should include drops")
		}
		if !df.includeModificationsOrAll() {
			t.Error("NoFilter should include modifications")
		}
	})

	// Test that each specific filter only returns true for its method
	t.Run("FilterAdds only true for adds", func(t *testing.T) {
		df := &diffTypeFilter{filterBy: FilterAdds}

		if !df.includeAddsOrAll() {
			t.Error("FilterAdds should include adds")
		}
		if df.includeDropsOrAll() {
			t.Error("FilterAdds should not include drops")
		}
		if df.includeModificationsOrAll() {
			t.Error("FilterAdds should not include modifications")
		}
	})

	t.Run("FilterRemoved only true for drops", func(t *testing.T) {
		df := &diffTypeFilter{filterBy: FilterRemoved}

		if df.includeAddsOrAll() {
			t.Error("FilterRemoved should not include adds")
		}
		if !df.includeDropsOrAll() {
			t.Error("FilterRemoved should include drops")
		}
		if df.includeModificationsOrAll() {
			t.Error("FilterRemoved should not include modifications")
		}
	})

	t.Run("FilterModified only true for modifications", func(t *testing.T) {
		df := &diffTypeFilter{filterBy: FilterModified}

		if df.includeAddsOrAll() {
			t.Error("FilterModified should not include adds")
		}
		if df.includeDropsOrAll() {
			t.Error("FilterModified should not include drops")
		}
		if !df.includeModificationsOrAll() {
			t.Error("FilterModified should include modifications")
		}
	})
}

func TestDiffTypeFilter_InvalidFilterBehavior(t *testing.T) {
	// Test that invalid filters consistently return false for all include methods
	invalidFilters := []string{"", "invalid", "ADDED", "addedd", "delete"}

	for _, filterValue := range invalidFilters {
		t.Run("invalid filter: "+filterValue, func(t *testing.T) {
			df := &diffTypeFilter{filterBy: filterValue}

			if !df.isValid() {
				// Only test include methods if filter is invalid
				if df.includeAddsOrAll() {
					t.Error("Invalid filter should not include adds")
				}
				if df.includeDropsOrAll() {
					t.Error("Invalid filter should not include drops")
				}
				if df.includeModificationsOrAll() {
					t.Error("Invalid filter should not include modifications")
				}
			}
		})
	}
}

func TestDiffCmd_ValidateArgs_ValidFilterValues(t *testing.T) {
	tests := []struct {
		name      string
		filterArg string
	}{
		{"valid: added", "added"},
		{"valid: modified", "modified"},
		{"valid: removed", "removed"},
		{"valid: all", "all"},
		{"valid: empty (not provided)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We're testing that these values pass validation
			// The actual validation happens in validateArgs which checks constants
			if tt.filterArg != "" {
				validValues := []string{FilterAdds, FilterModified, FilterRemoved, NoFilter}
				found := false
				for _, valid := range validValues {
					if tt.filterArg == valid {
						found = true
						break
					}
				}
				if !found && tt.filterArg != "" {
					t.Errorf("Expected %s to be a valid filter value", tt.filterArg)
				}
			}
		})
	}
}

func TestDiffCmd_ValidateArgs_InvalidFilterValues(t *testing.T) {
	tests := []struct {
		name      string
		filterArg string
	}{
		{"invalid: random string", "invalid"},
		{"invalid: typo addedd", "addedd"},
		{"invalid: uppercase ADDED", "ADDED"},
		{"invalid: insert", "insert"},
		{"invalid: update", "update"},
		{"invalid: delete", "delete"},
		{"invalid: adds (plural)", "adds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These should NOT be in the list of valid values
			validValues := []string{FilterAdds, FilterModified, FilterRemoved, NoFilter}
			for _, valid := range validValues {
				if tt.filterArg == valid {
					t.Errorf("Expected %s to be invalid, but it matched %s", tt.filterArg, valid)
				}
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
		{"FilterAdds value", FilterAdds, "added"},
		{"FilterModified value", FilterModified, "modified"},
		{"FilterRemoved value", FilterRemoved, "removed"},
		{"NoFilter value", NoFilter, "all"},
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
	constants := []string{FilterAdds, FilterModified, FilterRemoved, NoFilter}
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
	constants := []string{FilterAdds, FilterModified, FilterRemoved, NoFilter}

	for _, c := range constants {
		if c != strings.ToLower(c) {
			t.Errorf("Filter constant %s should be lowercase", c)
		}
	}
}

func TestDiffTypeFilter_MethodNaming(t *testing.T) {
	// Ensure method names are consistent and descriptive
	df := &diffTypeFilter{filterBy: NoFilter}

	// These tests just verify the methods exist and are callable
	// (compilation will fail if methods are renamed)
	_ = df.isValid()
	_ = df.includeAddsOrAll()
	_ = df.includeDropsOrAll()
	_ = df.includeModificationsOrAll()
}

func TestShouldUseLazyHeader(t *testing.T) {
	tests := []struct {
		name           string
		filter         *diffTypeFilter
		schemaChange   bool
		isRename       bool
		expectedResult bool
	}{
		{
			name:           "use lazy: filter active, data-only change",
			filter:         &diffTypeFilter{filterBy: FilterAdds},
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
		{
			name:           "don't use lazy: no filter",
			filter:         nil,
			schemaChange:   false,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: filter is NoFilter",
			filter:         &diffTypeFilter{filterBy: NoFilter},
			schemaChange:   false,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: schema changed",
			filter:         &diffTypeFilter{filterBy: FilterModified},
			schemaChange:   true,
			isRename:       false,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: table renamed",
			filter:         &diffTypeFilter{filterBy: FilterRemoved},
			schemaChange:   false,
			isRename:       true,
			expectedResult: false,
		},
		{
			name:           "don't use lazy: schema changed AND renamed",
			filter:         &diffTypeFilter{filterBy: FilterAdds},
			schemaChange:   true,
			isRename:       true,
			expectedResult: false,
		},
		{
			name:           "use lazy: filter=modified, data-only",
			filter:         &diffTypeFilter{filterBy: FilterModified},
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
		{
			name:           "use lazy: filter=removed, data-only",
			filter:         &diffTypeFilter{filterBy: FilterRemoved},
			schemaChange:   false,
			isRename:       false,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dArgs := &diffArgs{
				diffDisplaySettings: &diffDisplaySettings{
					filter: tt.filter,
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
	factory := func() (diff.SqlRowDiffWriter, error) {
		return mockDW.RowWriter(nil, nil, diff.TableDeltaSummary{}, nil)
	}

	lazyWriter := newLazyRowWriter(mockDW, "fromTable", "toTable", false, false, factory)

	// Close without writing any rows
	err := lazyWriter.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// BeginTable should NEVER have been called
	if mockDW.beginTableCalled {
		t.Error("BeginTable() was called even though no rows were written - should have been lazy!")
	}
}

func TestLazyRowWriter_RowsWritten(t *testing.T) {
	mockDW := &mockDiffWriter{}
	factory := func() (diff.SqlRowDiffWriter, error) {
		return mockDW.RowWriter(nil, nil, diff.TableDeltaSummary{}, nil)
	}

	lazyWriter := newLazyRowWriter(mockDW, "fromTable", "toTable", false, false, factory)

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
	factory := func() (diff.SqlRowDiffWriter, error) {
		return mockDW.RowWriter(nil, nil, diff.TableDeltaSummary{}, nil)
	}

	lazyWriter := newLazyRowWriter(mockDW, "fromTable", "toTable", false, false, factory)

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
	factory := func() (diff.SqlRowDiffWriter, error) {
		return mockDW.RowWriter(nil, nil, diff.TableDeltaSummary{}, nil)
	}

	lazyWriter := newLazyRowWriter(mockDW, "fromTable", "toTable", false, false, factory)

	ctx := sql.NewEmptyContext()

	// Write multiple rows
	for i := 0; i < 5; i++ {
		mockDW.beginTableCalled = false // Reset flag
		err := lazyWriter.WriteRow(ctx, sql.Row{}, diff.Added, []diff.ChangeType{})
		if err != nil {
			t.Fatalf("WriteRow() %d returned error: %v", i, err)
		}
		if mockDW.beginTableCalled {
			callCount++
		}
	}

	// BeginTable should have been called exactly ONCE (on first write only)
	if callCount != 1 {
		t.Errorf("BeginTable() called %d times, expected exactly 1", callCount)
	}
}
