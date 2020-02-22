package setalgebra

import (
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptySet(t *testing.T) {
	empty := EmptySet{}
	testVal := mustFiniteSet(NewFiniteSet(types.Format_Default, types.String("test")))

	union, err := empty.Union(testVal)
	assert.NoError(t, err)

	assert.Equal(t, testVal, union)

	intersection, err := empty.Intersect(testVal)
	assert.NoError(t, err)

	assert.Equal(t, empty, intersection)
}

func TestUniversalSet(t *testing.T) {
	universal := UniversalSet{}
	testVal := mustFiniteSet(NewFiniteSet(types.Format_Default, types.String("test")))

	union, err := universal.Union(testVal)
	assert.NoError(t, err)

	assert.Equal(t, universal, union)

	intersection, err := universal.Intersect(testVal)
	assert.NoError(t, err)

	assert.Equal(t, testVal, intersection)
}
