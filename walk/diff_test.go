package walk

import (
	"testing"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestDiff(t *testing.T) {
	assert := assert.New(t)

	// {"string": "string",
	//  "list": [false true],
	//  "map": {"nested": "string"}
	//  "mtlist": []
	//  "set": [5 7 8]
	// }
	from := types.NewMap(
		types.NewString("string"), types.NewString("string"),
		types.NewString("map"), types.NewMap(types.NewString("nested"), types.NewString("string")),
		types.NewString("mtlist"), types.NewList())

	setKey := types.NewString("set")
	setElem := types.Int32(7)
	setVal := types.NewSet(setElem)
	to := from.Set(setKey, setVal)

	var hashes []string
	for _, r := range Diff(from, to) {
		hashes = append(hashes, r.String())
	}

	assert.Contains(hashes, setKey.Ref().String())
	assert.Contains(hashes, setElem.Ref().String())
	assert.Contains(hashes, setVal.Ref().String())

	assert.Empty(Diff(from, from))
}
