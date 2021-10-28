// Copyright 2021 Dolthub, Inc.
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

package val

import "github.com/dolthub/dolt/go/store/pool"

type TupleBuilder struct {
	desc TupleDesc

	buf [MaxTupleDataSize]byte
	off ByteSize

	fields [MaxTupleFields][]byte
	maxIdx int
}

func NewTupleBuilder(desc TupleDesc) *TupleBuilder {
	return &TupleBuilder{
		desc:   desc,
		maxIdx: -1,
	}
}

func (tb *TupleBuilder) Tuple(pool pool.BuffPool) (tup Tuple) {
	tup = NewTuple(pool, tb.fields[:tb.maxIdx+1]...)
	tb.Recycle()
	return
}

func (tb *TupleBuilder) Recycle() {
	tb.off = 0
	tb.maxIdx = -1
}

func (tb *TupleBuilder) PutBool(i int, v bool) {
	tb.desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeBool(tb.fields[i], v)
	tb.off += 1
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutInt8(i int, v int8) {
	tb.desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeInt8(tb.fields[i], v)
	tb.off += 1
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutUint8(i int, v uint8) {
	tb.desc.expectEncoding(i, Uint8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeUint8(tb.fields[i], v)
	tb.off += 1
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutInt16(i int, v int16) {
	tb.desc.expectEncoding(i, Int16Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+2]
	writeInt16(tb.fields[i], v)
	tb.off += 2
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutUint16(i int, v uint16) {
	tb.desc.expectEncoding(i, Uint16Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+2]
	writeUint16(tb.fields[i], v)
	tb.off += 2
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutInt32(i int, v int32) {
	tb.desc.expectEncoding(i, Int32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeInt32(tb.fields[i], v)
	tb.off += 4
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutUint32(i int, v uint32) {
	tb.desc.expectEncoding(i, Uint32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeUint32(tb.fields[i], v)
	tb.off += 4
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutInt64(i int, v int64) {
	tb.desc.expectEncoding(i, Int64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeInt64(tb.fields[i], v)
	tb.off += 8
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutUint64(i int, v uint64) {
	tb.desc.expectEncoding(i, Uint64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeUint64(tb.fields[i], v)
	tb.off += 8
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutFloat32(i int, v float32) {
	tb.desc.expectEncoding(i, Float32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeFloat32(tb.fields[i], v)
	tb.off += 4
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutFloat64(i int, v float64) {
	tb.desc.expectEncoding(i, Float64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeFloat64(tb.fields[i], v)
	tb.off += 8
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutString(i int, v string) {
	tb.desc.expectEncoding(i, StringEnc)
	sz := ByteSize(len(v))
	tb.fields[i] = tb.buf[tb.off : tb.off+sz]
	writeString(tb.fields[i], v, tb.desc.types[i].Coll)
	tb.off += sz
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) PutBytes(i int, v []byte) {
	tb.desc.expectEncoding(i, BytesEnc)
	sz := ByteSize(len(v))
	tb.fields[i] = tb.buf[tb.off : tb.off+sz]
	writeBytes(tb.fields[i], v, tb.desc.types[i].Coll)
	tb.off += sz
	tb.updateMaxIdx(i)
}

func (tb *TupleBuilder) updateMaxIdx(i int) {
	if tb.maxIdx < i {
		tb.maxIdx = i
	}
}
