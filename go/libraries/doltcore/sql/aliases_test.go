
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

func TestColumnAliases(t *testing.T) {
	t.Run("single entry", func(t *testing.T) {
		a := NewAliases()
		column := QualifiedColumn{"t", "col"}
		alias := "alias"
		a.AddColumnAlias(column, alias)

		columnAlias, ok := a.GetColumnAlias(column)
		assert.True(t, ok)
		assert.Equal(t, alias, columnAlias)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{"t", "col2"})
		assert.False(t, ok)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{"t2", alias})
		assert.False(t, ok)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{})
		assert.False(t, ok)

		qc, err := a.GetColumnForAlias(alias)
		require.NoError(t, err)
		assert.Equal(t, column, qc)
	})

	t.Run("multiple entries", func(t *testing.T) {
		a := NewAliases()

		column1 := QualifiedColumn{"t", "col1"}
		column2 := QualifiedColumn{"t", "col2"}
		column3 := QualifiedColumn{"t", "col3"}
		alias1 := "alias1"
		alias2 := "alias2"
		a.AddColumnAlias(column1, alias1)
		a.AddColumnAlias(column2, alias2)
		a.AddColumnAlias(column3, alias2)

		columnAlias, ok := a.GetColumnAlias(column1)
		assert.True(t, ok)
		assert.Equal(t, alias1, columnAlias)
		columnAlias, ok = a.GetColumnAlias(column2)
		assert.True(t, ok)
		assert.Equal(t, alias2, columnAlias)
		columnAlias, ok = a.GetColumnAlias(column3)
		assert.True(t, ok)
		assert.Equal(t, alias2, columnAlias)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{"t", "colx"})
		assert.False(t, ok)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{"t2", alias1})
		assert.False(t, ok)

		columnAlias, ok = a.GetColumnAlias(QualifiedColumn{})
		assert.False(t, ok)

		qc, err := a.GetColumnForAlias(alias1)
		require.NoError(t, err)
		assert.Equal(t, column1, qc)

		_, err = a.GetColumnForAlias(alias2)
		require.Error(t, err)
		assert.Equal(t, err, errFmt(AmbiguousColumnErrFmt, alias2))
	})
}
