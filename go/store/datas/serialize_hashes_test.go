// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
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

	err := serializeHashes(b, input)
	assert.NoError(t, err)
	output, err := deserializeHashes(b)
	assert.NoError(t, err)
	assert.Len(t, output, len(input), "Output has different number of elements than input: %v, %v", output, input)
	for _, h := range output {
		_, present := input[h]
		assert.True(t, present, "%s is in output but not in input", h)
	}
}
