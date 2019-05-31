
package sql

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableAliases(t *testing.T) {
	a := NewAliases()
	alias1 := "alias"
	table1 := "table1"
	require.NoError(t, a.AddTableAlias(table1, alias1))
	alias2 := "alias2"
	alias3 := "alias3"
	table2 := "table2"
	require.NoError(t, a.AddTableAlias(table2, alias2))
	require.NoError(t, a.AddTableAlias(table2, alias3))

	assert.Equal(t, table1, a.TablesByAlias[alias1])
	assert.Equal(t, table2, a.TablesByAlias[alias2])

	assert.Equal(t, 1, len(a.AliasesByTable[table1]))
	assert.Equal(t, alias1, a.AliasesByTable[table1][0])

	assert.Equal(t, 2, len(a.AliasesByTable[table2]))
	assert.Equal(t, alias2, a.AliasesByTable[table2][0])
	assert.Equal(t, alias3, a.AliasesByTable[table2][1])

	err := a.AddTableAlias("table3", alias1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Duplicate table alias")
}