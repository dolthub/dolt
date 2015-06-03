package ref

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseError(t *testing.T) {
	assert := assert.New(t)

	r, err := Parse("foo")
	assert.Error(err)
	r, err = Parse("sha1")
	assert.Error(err)
	r, err = Parse("sha1-0")
	assert.Error(err)

	// too many digits
	r, err = Parse("sha1-00000000000000000000000000000000000000000")
	assert.Error(err)

	// 'g' not valid hex
	r, err = Parse("sha1-	000000000000000000000000000000000000000g")
	assert.Error(err)

	// sha2 not supported
	r, err = Parse("sha2-0000000000000000000000000000000000000000")
	assert.Error(err)

	r, err = Parse("sha1-0000000000000000000000000000000000000000")
	assert.NoError(err)
	assert.NotNil(r)
}

func TestEquals(t *testing.T) {
	assert := assert.New(t)

	r0 := MustParse("sha1-0000000000000000000000000000000000000000")
	r01 := MustParse("sha1-0000000000000000000000000000000000000000")
	r1 := MustParse("sha1-0000000000000000000000000000000000000001")

	assert.True(r0.Equals(r01))
	assert.True(r01.Equals(r0))
	assert.False(r1.Equals(r0))
	assert.False(r0.Equals(r1))
}

func TestString(t *testing.T) {
	s := "sha1-0123456789abcdef0123456789abcdef01234567"
	r := MustParse(s)
	assert.Equal(t, s, r.String())
}
