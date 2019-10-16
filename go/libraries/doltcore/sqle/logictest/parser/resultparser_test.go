package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseResultFile(t *testing.T) {
	entries, err := ParseResultFile("testdata/log.txt")
	assert.NoError(t, err)

	assert.NotEqual(t, 0, len(entries))
}
