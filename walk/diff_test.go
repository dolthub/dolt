package walk

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestGetReachabilitySetDiff(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.TestStore{}

	storeAndRef := func(v types.Value) (r ref.Ref) {
		r, err := types.WriteValue(v, cs)
		assert.NoError(err)
		return
	}

	// {"string": "string",
	//  "map": {"nested": "string"}
	//  "mtlist": []
	// }
	small := types.NewMap(
		types.NewString("string"), types.NewString("string"),
		types.NewString("map"), types.NewMap(types.NewString("nested"), types.NewString("string")),
		types.NewString("mtlist"), types.NewList())

	setVal := types.NewSet(types.Int32(7))
	big := small.Set(types.NewString("set"), setVal)

	var hashes []string
	for _, r := range GetReachabilitySetDiff(storeAndRef(small), storeAndRef(big), cs) {
		hashes = append(hashes, r.String())
	}

	assert.Contains(hashes, setVal.Ref().String())

	assert.Empty(GetReachabilitySetDiff(small.Ref(), small.Ref(), cs))
}
