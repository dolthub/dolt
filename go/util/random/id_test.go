package random

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

type testReader byte

func (r *testReader) Read(dest []byte) (int, error) {
	for i := 0; i < len(dest); i++ {
		dest[i] = byte(*r)
	}
	return len(dest), nil
}

func TestBasic(t *testing.T) {
	assert := assert.New(t)

	func() {
		var r testReader
		oldReader := reader
		reader = &r
		defer func() {
			reader = oldReader
		}()

		r = testReader(byte(0x00))
		assert.Equal("00000000000000000000000000000000", Id())
		r = testReader(byte(0x01))
		assert.Equal("01010101010101010101010101010101", Id())
		r = testReader(byte(0xFF))
		assert.Equal("ffffffffffffffffffffffffffffffff", Id())
	}()

	one := Id()
	two := Id()
	assert.NotEqual(one, two)
}
