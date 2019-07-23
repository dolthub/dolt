// Copyright 2019 Liquidata, Inc.
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


package sql

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableAliases(t *testing.T) {
	a := NewAliases()
	alias1 := "alias"
	table1 := "Table1"
	require.NoError(t, a.AddTableAlias(table1, alias1))
	alias2 := "alias2"
	alias3 := "alias3"
	table2 := "Table2"
	require.NoError(t, a.AddTableAlias(table2, alias2))
	require.NoError(t, a.AddTableAlias(table2, alias3))

	var tableName string
	var ok bool

	tableName, ok = a.GetTableByAlias(alias1)
	assert.True(t, ok)
	assert.Equal(t, table1, tableName)

	tableName, ok = a.GetTableByAlias(alias2)
	assert.True(t, ok)
	assert.Equal(t, table2, tableName)

	tableName, ok = a.GetTableByAlias(alias3)
	assert.True(t, ok)
	assert.Equal(t, table2, tableName)

	// aliases are case-insensitive
	tableName, ok = a.GetTableByAlias("ALIAS3")
	assert.True(t, ok)
	assert.Equal(t, table2, tableName)

	tableName, ok = a.GetTableByAlias("not a thing")
	assert.False(t, ok)

	err := a.AddTableAlias("Table3", alias1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Non-unique")
}
