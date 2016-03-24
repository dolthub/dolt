package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendCompactedJSON(t *testing.T) {
	assert := assert.New(t)

	buf := &bytes.Buffer{}
	s := "a<b>c&d"
	appendCompactedJSON(buf, s)
	assert.Equal(`"`+s+`"`, buf.String())
}

func TestNormalizeJSONStrings(t *testing.T) {
	assert := assert.New(t)

	test := func(expected, s string) {
		actual := normalizeJSONStrings(s)
		assert.Equal(expected, actual)
	}

	test("", "")
	test("abcd", "abcd")
	test("abcdefgh", "abcdefgh")

	test("<", `\u003c`)
	test(">", `\u003e`)
	test("&", `\u0026`)
	test("\u2028", `\u2028`)
	test("\u2029", `\u2029`)

	test(`a\b`, `a\b`)
	test(`a\ub`, `a\ub`)
	test("a<b", `a\u003cb`)
	test("a>b", `a\u003eb`)
	test("a&b", `a\u0026b`)
	test("<>&\u2028\u2029", `\u003c\u003e\u0026\u2028\u2029`)

	test(`\u0097`, `\u0097`)
	test("\u0097", "\u0097")
}
