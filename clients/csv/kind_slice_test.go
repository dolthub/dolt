package csv

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestKindSliceJSON(t *testing.T) {
	assert := assert.New(t)

	ks := KindSlice{types.NumberKind, types.StringKind, types.BoolKind}
	b, err := json.Marshal(&ks)
	assert.NoError(err)

	assert.Equal(fmt.Sprintf("[%d,%d,%d]", ks[0], ks[1], ks[2]), string(b))

	var uks KindSlice
	err = json.Unmarshal(b, &uks)
	assert.Equal(ks, uks)
}
