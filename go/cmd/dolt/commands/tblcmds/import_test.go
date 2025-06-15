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

package tblcmds

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func TestValidatePrimaryKeysAgainstSchema(t *testing.T) {
	// Create a test schema with columns: id, name, email, age
	testCols := []schema.Column{
		{Name: "id", Tag: 0, IsPartOfPK: true},
		{Name: "name", Tag: 1, IsPartOfPK: false},
		{Name: "email", Tag: 2, IsPartOfPK: false},
		{Name: "age", Tag: 3, IsPartOfPK: false},
	}
	testSchema, err := schema.NewSchema(
		schema.NewColCollection(testCols...),
		nil, // pkOrdinals - nil means use default ordinals based on IsPartOfPK
		schema.Collation_Default,
		nil, // indexes
		nil, // checks
	)
	require.NoError(t, err)

	tests := []struct {
		name          string
		primaryKeys   []string
		schema        schema.Schema
		expectError   bool
		errorContains string
	}{
		{
			name:        "valid single primary key",
			primaryKeys: []string{"id"},
			schema:      testSchema,
			expectError: false,
		},
		{
			name:        "valid multiple primary keys",
			primaryKeys: []string{"id", "name"},
			schema:      testSchema,
			expectError: false,
		},
		{
			name:        "empty primary keys",
			primaryKeys: []string{},
			schema:      testSchema,
			expectError: false,
		},
		{
			name:          "invalid single primary key",
			primaryKeys:   []string{"invalid_col"},
			schema:        testSchema,
			expectError:   true,
			errorContains: "primary key 'invalid_col' not found in import file. Available columns: id, name, email, age",
		},
		{
			name:          "mix of valid and invalid primary keys",
			primaryKeys:   []string{"id", "invalid_col1", "name", "invalid_col2"},
			schema:        testSchema,
			expectError:   true,
			errorContains: "primary keys [invalid_col1 invalid_col2] not found in import file. Available columns: id, name, email, age",
		},
		{
			name:          "all invalid primary keys",
			primaryKeys:   []string{"col1", "col2", "col3"},
			schema:        testSchema,
			expectError:   true,
			errorContains: "primary keys [col1 col2 col3] not found in import file. Available columns: id, name, email, age",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePrimaryKeysAgainstSchema(tt.primaryKeys, tt.schema, rowconv.NameMapper{})

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePrimaryKeysAgainstSchemaColumnOrder(t *testing.T) {
	// Test that available columns are listed in a consistent order
	testCols := []schema.Column{
		{Name: "zebra", Tag: 3, IsPartOfPK: false},
		{Name: "alpha", Tag: 0, IsPartOfPK: true},
		{Name: "beta", Tag: 1, IsPartOfPK: false},
		{Name: "gamma", Tag: 2, IsPartOfPK: false},
	}
	testSchema, err := schema.NewSchema(
		schema.NewColCollection(testCols...),
		nil, // pkOrdinals - nil means use default ordinals based on IsPartOfPK
		schema.Collation_Default,
		nil, // indexes
		nil, // checks
	)
	require.NoError(t, err)

	err = validatePrimaryKeysAgainstSchema([]string{"invalid"}, testSchema, rowconv.NameMapper{})
	assert.Error(t, err)
	// The implementation returns a detailed error with available columns
	assert.Contains(t, err.Error(), "primary key 'invalid' not found in import file. Available columns: zebra, alpha, beta, gamma")
}

func TestValidatePrimaryKeysWithNameMapping(t *testing.T) {
	// Create a test schema with columns: user_id, user_name, user_email
	testCols := []schema.Column{
		{Name: "user_id", Tag: 0, IsPartOfPK: true},
		{Name: "user_name", Tag: 1, IsPartOfPK: false},
		{Name: "user_email", Tag: 2, IsPartOfPK: false},
	}
	testSchema, err := schema.NewSchema(
		schema.NewColCollection(testCols...),
		nil,
		schema.Collation_Default,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Create a name mapper that maps user_id -> id, user_name -> name, user_email -> email
	nameMapper := rowconv.NameMapper{
		"user_id":    "id",
		"user_name":  "name",
		"user_email": "email",
	}

	tests := []struct {
		name        string
		primaryKeys []string
		expectError bool
	}{
		{
			name:        "primary key using mapped name",
			primaryKeys: []string{"id"}, // mapped from user_id
			expectError: false,
		},
		{
			name:        "primary key using original name",
			primaryKeys: []string{"user_id"}, // original column name
			expectError: false,
		},
		{
			name:        "multiple primary keys with mixed names",
			primaryKeys: []string{"id", "name"}, // mapped names
			expectError: false,
		},
		{
			name:        "invalid primary key not in mapping",
			primaryKeys: []string{"invalid_col"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePrimaryKeysAgainstSchema(tt.primaryKeys, testSchema, nameMapper)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
