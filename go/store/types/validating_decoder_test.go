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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
)

func TestValidatingBatchingSinkDecode(t *testing.T) {
	v := Float(42)
	c, err := EncodeValue(v, Format_7_18)
	assert.NoError(t, err)
	storage := &chunks.TestStorage{}
	vdc := NewValidatingDecoder(storage.NewView())

	dc, err := vdc.Decode(&c)
	assert.NoError(t, err)
	assert.True(t, v.Equals(*dc.Value))
}

func assertPanicsOnInvalidChunk(t *testing.T, data []interface{}) {
	storage := &chunks.TestStorage{}
	vs := NewValueStore(storage.NewView())
	dataAsByteSlice := toBinaryNomsReaderData(data)
	dec := newValueDecoder(dataAsByteSlice, vs)
	v, err := dec.readValue(Format_7_18)
	assert.NoError(t, err)

	c, err := EncodeValue(v, Format_7_18)
	assert.NoError(t, err)
	vdc := NewValidatingDecoder(storage.NewView())

	assert.Panics(t, func() {
		_, _ = vdc.Decode(&c)
	})
}

func TestValidatingBatchingSinkDecodeInvalidUnion(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(UnionKind), uint64(2) /* len */, uint8(FloatKind), uint8(BoolKind),
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldOrder(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint64(2), /* len */
		"b", "a",
		uint8(FloatKind), uint8(FloatKind),
		false, false,
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S ", uint64(0), /* len */
	}
	assertPanicsOnInvalidChunk(t, data)
}

func TestValidatingBatchingSinkDecodeInvalidStructFieldName(t *testing.T) {
	data := []interface{}{
		uint8(TypeKind),
		uint8(StructKind), "S", uint64(1), /* len */
		"b ", uint8(FloatKind), false,
	}
	assertPanicsOnInvalidChunk(t, data)
}
