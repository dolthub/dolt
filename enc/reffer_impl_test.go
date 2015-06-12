package enc

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestRefferImpl(t *testing.T) {
	assert := assert.New(t)
	input := "j false\n"
	h := ref.NewHash()
	h.Write([]byte(input))
	expected := ref.FromHash(h)
	actual := types.Reffer(types.Bool(false))
	assert.Equal(expected, actual)
}
