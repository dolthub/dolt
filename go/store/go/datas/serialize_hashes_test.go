// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

func TestHashRoundTrip(t *testing.T) {
	b := &bytes.Buffer{}
	input := chunks.ReadBatch{
		hash.Parse("00000000000000000000000000000000"): nil,
		hash.Parse("00000000000000000000000000000001"): nil,
		hash.Parse("00000000000000000000000000000002"): nil,
		hash.Parse("00000000000000000000000000000003"): nil,
	}
	defer input.Close()

	serializeHashes(b, input)
	output := deserializeHashes(b)
	assert.Len(t, output, len(input), "Output has different number of elements than input: %v, %v", output, input)
	for _, h := range output {
		_, present := input[h]
		assert.True(t, present, "%s is in output but not in input", h)
	}
}
