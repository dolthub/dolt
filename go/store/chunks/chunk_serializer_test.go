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

package chunks

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeRoundTrip(t *testing.T) {
	assert := assert.New(t)
	inputs := [][]byte{[]byte("abc"), []byte("def")}
	chnx := make([]Chunk, len(inputs))
	for i, data := range inputs {
		chnx[i] = NewChunk(data)
	}

	buf := &bytes.Buffer{}
	Serialize(chnx[0], buf)
	Serialize(chnx[1], buf)

	chunkChan := make(chan *Chunk)
	go func() {
		defer close(chunkChan)
		err := Deserialize(bytes.NewReader(buf.Bytes()), chunkChan)
		assert.NoError(err)
	}()

	for c := range chunkChan {
		assert.Equal(chnx[0].Hash(), c.Hash())
		chnx = chnx[1:]
	}
	assert.Len(chnx, 0)
}

func TestBadSerialization(t *testing.T) {
	bad := []byte{0, 1} // Not enough bytes to read first length
	ch := make(chan *Chunk)
	defer close(ch)
	assert.Error(t, Deserialize(bytes.NewReader(bad), ch))
}
