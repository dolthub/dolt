// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package encodedbytes

import (
	"bytes"
	"errors"
	iconv "github.com/djimenez/iconv-go"
)

const (
	BytesPerInt     = 4
	SynchByteLength = 7
	NormByteLength  = 8
	NativeEncoding  = 3
)

type Encoding struct {
	Name       string
	NullLength int
}

var (
	EncodingMap = [...]Encoding{
		{Name: "ISO-8859-1", NullLength: 1},
		{Name: "UTF-16", NullLength: 2},
		{Name: "UTF-16BE", NullLength: 2},
		{Name: "UTF-8", NullLength: 1},
	}
	Decoders = make([]*iconv.Converter, len(EncodingMap))
	Encoders = make([]*iconv.Converter, len(EncodingMap))
)

func init() {
	n := EncodingForIndex(NativeEncoding)
	for i, e := range EncodingMap {
		Decoders[i], _ = iconv.NewConverter(e.Name, n)
		Encoders[i], _ = iconv.NewConverter(n, e.Name)
	}
}

// Form an integer from concatenated bits
func ByteInt(buf []byte, base uint) (i uint32, err error) {
	if len(buf) > BytesPerInt {
		err = errors.New("byte integer: invalid []byte length")
		return
	}

	for _, b := range buf {
		if base < NormByteLength && b >= (1<<base) {
			err = errors.New("byte integer: exceed max bit")
			return
		}

		i = (i << base) | uint32(b)
	}

	return
}

func SynchInt(buf []byte) (i uint32, err error) {
	i, err = ByteInt(buf, SynchByteLength)
	return
}

func NormInt(buf []byte) (i uint32, err error) {
	i, err = ByteInt(buf, NormByteLength)
	return
}

// Form a byte slice from an integer
func IntBytes(n uint32, base uint) []byte {
	mask := uint32(1<<base - 1)
	bytes := make([]byte, BytesPerInt)

	for i, _ := range bytes {
		bytes[len(bytes)-i-1] = byte(n & mask)
		n >>= base
	}

	return bytes
}

func SynchBytes(n uint32) []byte {
	return IntBytes(n, SynchByteLength)
}

func NormBytes(n uint32) []byte {
	return IntBytes(n, NormByteLength)
}

func EncodingForIndex(b byte) string {
	encodingIndex := int(b)
	if encodingIndex < 0 || encodingIndex > len(EncodingMap) {
		encodingIndex = 0
	}

	return EncodingMap[encodingIndex].Name
}

func EncodingNullLengthForIndex(b byte) int {
	encodingIndex := int(b)
	if encodingIndex < 0 || encodingIndex > len(EncodingMap) {
		encodingIndex = 0
	}

	return EncodingMap[encodingIndex].NullLength
}

func IndexForEncoding(e string) byte {
	for i, v := range EncodingMap {
		if v.Name == e {
			return byte(i)
		}
	}

	return 0
}

func nullIndex(data []byte, encoding byte) (atIndex, afterIndex int) {
	byteCount := EncodingNullLengthForIndex(encoding)
	limit := len(data)
	null := bytes.Repeat([]byte{0x0}, byteCount)

	for i, _ := range data[:limit/byteCount] {
		atIndex = byteCount * i
		afterIndex = atIndex + byteCount

		if bytes.Equal(data[atIndex:afterIndex], null) {
			return
		}
	}

	atIndex = -1
	afterIndex = -1
	return
}

func EncodedDiff(newEncoding byte, newString string, oldEncoding byte, oldString string) (int, error) {
	newEncodedString, err := Encoders[newEncoding].ConvertString(newString)
	if err != nil {
		return 0, err
	}

	oldEncodedString, err := Encoders[oldEncoding].ConvertString(oldString)
	if err != nil {
		return 0, err
	}

	return len(newEncodedString) - len(oldEncodedString), nil
}
