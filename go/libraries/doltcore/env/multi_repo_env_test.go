package env

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDirToDBName(t *testing.T) {
	tests := map[string]string {
		"irs": "irs",
		"corona-virus": "corona_virus",
		"  fake - name     ": "fake_name",
	}

	for path, expected := range tests {
		actual := dirToDBName(path)
		assert.Equal(t, actual, expected)
	}
}
