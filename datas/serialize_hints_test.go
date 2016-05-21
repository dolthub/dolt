package datas

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/hash"
	"github.com/stretchr/testify/assert"
)

func TestHintRoundTrip(t *testing.T) {
	b := &bytes.Buffer{}
	input := map[hash.Hash]struct{}{
		hash.Parse("sha1-0000000000000000000000000000000000000000"): struct{}{},
		hash.Parse("sha1-0000000000000000000000000000000000000001"): struct{}{},
		hash.Parse("sha1-0000000000000000000000000000000000000002"): struct{}{},
		hash.Parse("sha1-0000000000000000000000000000000000000003"): struct{}{},
	}
	serializeHints(b, input)
	output := deserializeHints(b)
	assert.Len(t, output, len(input), "Output has different number of elements than input: %v, %v", output, input)
	for h := range output {
		_, present := input[h]
		assert.True(t, present, "%s is in output but not in input", h)
	}
}
