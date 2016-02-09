package gherkin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringDataToTable(t *testing.T) {
	s := StringData("| a | b |\n| 1 | 2 |\n| 3 | 4 |")
	tab := TabularData{
		[]string{"a", "b"},
		[]string{"1", "2"},
		[]string{"3", "4"},
	}

	assert.Equal(t, tab, s.ToTable())
}

func TestTabularDataToMap(t *testing.T) {
	tab := TabularData{
		[]string{"a", "b", "c", "d"},
		[]string{"1", "2", "3", "4"},
		[]string{"5", "6", "7", "8"},
		[]string{"9", "A", "B", "C"},
	}

	m := TabularDataMap{
		"a": []string{"1", "5", "9"},
		"b": []string{"2", "6", "A"},
		"c": []string{"3", "7", "B"},
		"d": []string{"4", "8", "C"},
	}

	assert.Equal(t, m, tab.ToMap())
	assert.Equal(t, 3, m.NumRows())
}

func TestTabularDataMapEmpty(t *testing.T) {
	var tab TabularData
	var m TabularDataMap

	// only headers
	tab = TabularData{[]string{"a", "b", "c", "d"}}
	m = TabularDataMap{}
	assert.Equal(t, m, tab.ToMap())
	assert.Equal(t, 0, m.NumRows())

	// completely empty
	tab = TabularData{}
	m = TabularDataMap{}
	assert.Equal(t, m, tab.ToMap())
	assert.Equal(t, 0, m.NumRows())
}

func TestScenarioFilters(t *testing.T) {
	f := &Feature{Tags: []string{}}
	s := Scenario{Tags: []string{"@a", "@b"}}
	assert.True(t, s.FilterMatched(f))
	assert.False(t, s.FilterMatched(f, "a"))
	assert.True(t, s.FilterMatched(f, "@a"))
	assert.True(t, s.FilterMatched(f, "@c", "@a"))
	assert.False(t, s.FilterMatched(f, "~@a"))
	assert.False(t, s.FilterMatched(f, "@a,@c"))
	assert.True(t, s.FilterMatched(f, "@a,@b", "@c"))

	s = Scenario{Tags: []string{}}
	assert.False(t, s.FilterMatched(f, "@a"))
}

func TestFeatureFilters(t *testing.T) {
	s := Feature{Tags: []string{"@a", "@b"}}
	assert.True(t, s.FilterMatched())
	assert.False(t, s.FilterMatched("a"))
	assert.True(t, s.FilterMatched("@a"))
	assert.True(t, s.FilterMatched("@c", "@a"))
	assert.False(t, s.FilterMatched("~@a"))
	assert.False(t, s.FilterMatched("@a,@c"))
	assert.True(t, s.FilterMatched("@a,@b", "@c"))
}
