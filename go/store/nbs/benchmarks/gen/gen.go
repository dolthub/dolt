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

package gen

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/dustin/go-humanize"

	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

const (
	OffsetTupleLen   = 24
	averageChunkSize = 4 * uint64(1<<10) // 4KB
)

func OpenOrGenerateDataFile(name string, totalData uint64) (data *os.File, err error) {
	data, err = os.Open(name)
	if os.IsNotExist(err) {
		data, _ = os.Create(name)
		fmt.Printf("Creating data file with %s\n", humanize.IBytes(totalData))
		generateData(data, totalData)
		_, err = data.Seek(0, io.SeekStart)
		d.Chk.NoError(err)
		return data, nil
	}
	d.Chk.NoError(err)

	info, err := data.Stat()
	d.Chk.NoError(err)
	if uint64(info.Size()) < totalData {
		data.Close()
		return nil, fmt.Errorf("%s is too small to benchmark with %s", name, humanize.IBytes(totalData))
	}
	return data, nil
}

func OpenOrBuildChunkMap(name string, data *os.File) *os.File {
	cm, err := os.Open(name)
	if os.IsNotExist(err) {
		cm, _ = os.Create(name)
		fmt.Printf("Chunking %s into chunk-map: %s ...", data.Name(), name)
		cc := chunk(cm, data)
		fmt.Println(cc, " chunks")

		_, err = cm.Seek(0, io.SeekStart)
		d.Chk.NoError(err)
		return cm
	}
	d.Chk.NoError(err)
	return cm
}

func generateData(w io.Writer, totalData uint64) {
	r := &randomByteReader{}

	buff := [humanize.MiByte]byte{}
	bs := buff[:]
	buffIdx := 0

	for bc := uint64(0); bc < totalData; bc++ {
		b, _ := r.ReadByte()
		bs[buffIdx] = b
		buffIdx++
		if buffIdx == int(humanize.MiByte) {
			io.Copy(w, bytes.NewReader(bs))
			buffIdx = 0
		}
	}
}

type randomByteReader struct {
	rand    *rand.Rand
	scratch [2 * averageChunkSize]byte
	pos     int
}

func (rbr *randomByteReader) ReadByte() (byte, error) {
	if rbr.rand == nil {
		rbr.rand = rand.New(rand.NewSource(0))
		rbr.pos = cap(rbr.scratch)
	}
	if rbr.pos >= cap(rbr.scratch) {
		rbr.rand.Read(rbr.scratch[:])
		rbr.pos = 0
	}
	b := rbr.scratch[rbr.pos]
	rbr.pos++
	return b, nil
}

func (rbr *randomByteReader) Close() error {
	return nil
}

type offsetTuple [OffsetTupleLen]byte

func chunk(w io.Writer, r io.Reader) (chunkCount int) {
	buff := [humanize.MiByte]byte{}
	bs := buff[:]
	buffIdx := uint64(0)
	rv := newRollingValueHasher()
	sha := sha512.New()
	ot := offsetTuple{}
	lastOffset := uint64(0)

	var err error
	var n int

	writeChunk := func() {
		chunkCount++
		var d []byte
		d = sha.Sum(d)
		copy(ot[:hash.ByteLen], d)

		chunkLength := uint32(buffIdx - lastOffset)

		binary.BigEndian.PutUint32(ot[hash.ByteLen:], chunkLength)

		io.Copy(w, bytes.NewReader(ot[:]))

		lastOffset = buffIdx
		sha.Reset()
	}

	for err == nil {
		n, err = io.ReadFull(r, bs)

		for i := uint64(0); i < uint64(n); i++ {
			b := bs[i]
			buffIdx++
			sha.Write(bs[i : i+1])

			if rv.HashByte(b) {
				writeChunk()
			}
		}
	}

	if lastOffset < buffIdx {
		writeChunk()
	}

	return
}
