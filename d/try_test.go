package d

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestTry(t *testing.T) {
	assert := assert.New(t)

	IsUsageError(assert, func() { Exp.Fail("hey-o") })

	assert.Panics(func() {
		Try(func() { Chk.Fail("hey-o") })
	})

	assert.Panics(func() {
		Try(func() { panic("hey-o") })
	})
}
