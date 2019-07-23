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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunk(t *testing.T) {
	c := NewChunk([]byte("abc"))
	h := c.Hash()
	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal(t, "rmnjb8cjc5tblj21ed4qs821649eduie", h.String())
}

func TestChunkWriteAfterCloseFails(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	assert.NoError(w.Close())
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func TestChunkWriteAfterChunkFails(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	_ = w.Chunk()
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Chunk() should barf!")
}

func TestChunkChunkCloses(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	w.Chunk()
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}
