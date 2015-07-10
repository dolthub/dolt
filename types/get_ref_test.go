package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestGetRef(t *testing.T) {
	assert := assert.New(t)
	input := "j false\n"
	h := ref.NewHash()
	h.Write([]byte(input))
	expected := ref.FromHash(h)
	actual := getRef(Bool(false))
	assert.Equal(expected, actual)
}
