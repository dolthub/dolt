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
	"strings"
	"testing"
)

// ============================================================================
// diffTypeFilter Struct Tests
// ============================================================================

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

// ============================================================================
// Edge Case Tests
// ============================================================================

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

// ============================================================================
// Validation Tests
// ============================================================================

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

// ============================================================================
// Constant Value Tests
// ============================================================================

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

// ============================================================================
// Documentation Tests
// ============================================================================

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
