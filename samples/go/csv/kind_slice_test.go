// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestKindSliceJSON(t *testing.T) {
	assert := assert.New(t)

	ks := KindSlice{types.NumberKind, types.StringKind, types.BoolKind}
	b, err := json.Marshal(&ks)
	assert.NoError(err)

	assert.Equal(fmt.Sprintf("[%d,%d,%d]", ks[0], ks[1], ks[2]), string(b))

	var uks KindSlice
	err = json.Unmarshal(b, &uks)
	assert.NoError(err, "error with json.Unmarshal")
	assert.Equal(ks, uks)
}
