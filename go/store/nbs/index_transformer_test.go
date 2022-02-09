// Copyright 2022 Dolthub, Inc.
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

package nbs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/test"
)

// minByteReader is a copy of smallerByteReader from testing/iotest
// but with a minimum read size of min bytes.

type minByteReader struct {
	r   io.Reader
	min int

	n   int
	off int
}

func (r *minByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	r.n = r.min + rand.Intn(r.min*100)

	n := r.n
	if n > len(p) {
		n = len(p)
	}
	n, err := r.r.Read(p[0:n])
	if err != nil && err != io.EOF {
		err = fmt.Errorf("Read(%d bytes at offset %d): %v", n, r.off, err)
	}
	r.off += n
	return n, err
}

// Altered from testing/iotest.TestReader to use minByteReader
func testReader(r io.Reader, content []byte) error {
	if len(content) > 0 {
		n, err := r.Read(nil)
		if n != 0 || err != nil {
			return fmt.Errorf("Read(0) = %d, %v, want 0, nil", n, err)
		}
	}

	data, err := io.ReadAll(&minByteReader{r: r, min: offsetSize})
	if err != nil {
		return err
	}
	if !bytes.Equal(data, content) {
		return fmt.Errorf("ReadAll(varied amounts) = %q\n\twant %q", data, content)
	}

	n, err := r.Read(make([]byte, offsetSize))
	if n != 0 || err != io.EOF {
		return fmt.Errorf("Read(offsetSize) at EOF = %v, %v, want 0, EOF", n, err)
	}

	return nil
}

func get32Bytes(src []uint32) []byte {
	dst := make([]byte, len(src)*uint32Size)
	for i, start, end := 0, 0, lengthSize; i < len(src); i, start, end = i+1, end, end+lengthSize {
		p := dst[start:end]
		binary.BigEndian.PutUint32(p, src[i])
	}
	return dst
}

func get64Bytes(src []uint64) []byte {
	dst := make([]byte, len(src)*uint64Size)
	for i, start, end := 0, 0, offsetSize; i < len(src); i, start, end = i+1, end, end+offsetSize {
		p := dst[start:end]
		binary.BigEndian.PutUint64(p, src[i])
	}
	return dst
}

func randomUInt32s(n int) []uint32 {
	out := make([]uint32, n)
	for i := 0; i < n; i++ {
		out[i] = uint32(rand.Intn(1000))
	}
	return out
}

func calcOffsets(arr []uint32) []uint64 {
	out := make([]uint64, len(arr))
	out[0] = uint64(arr[0])
	for i := 1; i < len(arr); i++ {
		out[i] = out[i-1] + uint64(arr[i])
	}
	return out
}

func TestOffsetReader(t *testing.T) {
	testSize := rand.Intn(10) + 1
	lengths := randomUInt32s(testSize)
	offsets := calcOffsets(lengths)

	lengthBytes := get32Bytes(lengths)
	offsetBytes := get64Bytes(offsets)

	t.Run("converts lengths into offsets", func(t *testing.T) {
		lengthsReader := bytes.NewReader(lengthBytes)
		offsetReader := NewOffsetsReader(lengthsReader)

		err := testReader(offsetReader, offsetBytes)
		require.NoError(t, err)
	})

	t.Run("err not enough bytes when expected", func(t *testing.T) {
		lengthsReader := bytes.NewReader(lengthBytes[:len(lengthBytes)-1])
		offsetReader := NewOffsetsReader(lengthsReader)
		_, err := io.ReadAll(offsetReader)
		require.ErrorAsf(t, err, &ErrNotEnoughBytes, "should return ErrNotEnoughBytes")
	})

	t.Run("fills provided buffer correctly", func(t *testing.T) {
		lengthsReader := bytes.NewReader(lengthBytes)
		offsetReader := NewOffsetsReader(lengthsReader)
		p := make([]byte, offsetSize)
		n, err := offsetReader.Read(p)
		require.NoError(t, err)
		require.Equal(t, offsetSize, n)
	})

	t.Run("works with io.ReadAll", func(t *testing.T) {
		lengthsReader := bytes.NewReader(lengthBytes[:lengthSize])
		offsetReader := NewOffsetsReader(lengthsReader)
		data, err := io.ReadAll(offsetReader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(data, offsetBytes[:offsetSize]))
	})
}

func TestIndexTransformer(t *testing.T) {
	chunkCount := rand.Intn(10) + 1
	lengths := randomUInt32s(chunkCount)
	offsets := calcOffsets(lengths)
	lengthBytes := get32Bytes(lengths)
	offsetBytes := get64Bytes(offsets)

	tupleBytes := test.RandomData(chunkCount * prefixTupleSize)
	suffixBytes := test.RandomData(chunkCount * addrSuffixSize)

	var inBytes []byte
	inBytes = append(inBytes, tupleBytes...)
	inBytes = append(inBytes, lengthBytes...)
	inBytes = append(inBytes, suffixBytes...)

	var outBytes []byte
	outBytes = append(outBytes, tupleBytes...)
	outBytes = append(outBytes, offsetBytes...)
	outBytes = append(outBytes, suffixBytes...)

	t.Run("only converts lengths into offsets", func(t *testing.T) {
		inReader := bytes.NewBuffer(inBytes)
		outReader := NewIndexTransformer(inReader, chunkCount)

		err := testReader(outReader, outBytes)
		require.NoError(t, err)
	})

}
