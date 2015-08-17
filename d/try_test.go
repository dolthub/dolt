package d

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTry(t *testing.T) {
	assert := assert.New(t)

	e := Try(func() { Exp.Fail("hey-o") })
	assert.IsType(UsageError{}, e)

	assert.Panics(func() {
		Try(func() { Chk.Fail("hey-o") })
	})

	assert.Panics(func() {
		Try(func() { panic("hey-o") })
	})
}
