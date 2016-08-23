// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestHintRoundTrip(t *testing.T) {
	b := &bytes.Buffer{}
	input := map[hash.Hash]struct{}{
		hash.Parse("00000000000000000000000000000000"): {},
		hash.Parse("00000000000000000000000000000001"): {},
		hash.Parse("00000000000000000000000000000002"): {},
		hash.Parse("00000000000000000000000000000003"): {},
	}
	serializeHints(b, input)
	output := deserializeHints(b)
	assert.Len(t, output, len(input), "Output has different number of elements than input: %v, %v", output, input)
	for h := range output {
		_, present := input[h]
		assert.True(t, present, "%s is in output but not in input", h)
	}
}
