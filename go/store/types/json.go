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

package types

import (
	"context"
	"fmt"
	"slices"

	"github.com/dolthub/dolt/go/store/d"
)

type JSON struct {
	valueImpl
}

// NewJSONDoc wraps |value| in a JSON value.
func NewJSONDoc(nbf *NomsBinFormat, vrw ValueReadWriter, value Value) (JSON, error) {
	w := newBinaryNomsWriter()
	if err := JSONKind.writeTo(&w, nbf); err != nil {
		return emptyJSONDoc(nbf), err
	}

	if err := value.writeTo(&w, nbf); err != nil {
		return emptyJSONDoc(nbf), err
	}

	return JSON{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

// emptyJSONDoc creates and empty JSON value.
func emptyJSONDoc(nbf *NomsBinFormat) JSON {
	w := newBinaryNomsWriter()
	if err := JSONKind.writeTo(&w, nbf); err != nil {
		d.PanicIfError(err)
	}

	return JSON{valueImpl{nil, nbf, w.data(), nil}}
}

// CopyOf creates a copy of a JSON.  This is necessary in cases where keeping a reference to the original JSON is
// preventing larger objects from being collected.
func (t JSON) CopyOf(vrw ValueReadWriter) JSON {
	buff := make([]byte, len(t.buff))
	offsets := make([]uint32, len(t.offsets))

	copy(buff, t.buff)
	copy(offsets, t.offsets)

	return JSON{
		valueImpl{
			buff:    buff,
			offsets: offsets,
			vrw:     vrw,
			nbf:     t.nbf,
		},
	}
}

// Empty implements the Emptyable interface.
func (t JSON) Empty() bool {
	return t.Len() == 0
}

// Format returns this values NomsBinFormat.
func (t JSON) Format() *NomsBinFormat {
	return t.format()
}

// Value implements the Value interface.
func (t JSON) Value(ctx context.Context) (Value, error) {
	return t, nil
}

func (t JSON) typeOf() (*Type, error) {
	return PrimitiveTypeMap[JSONKind], nil
}

// Kind implements the Valuable interface.
func (t JSON) Kind() NomsKind {
	return JSONKind
}

// Len implements the Value interface.
func (t JSON) Len() uint64 {
	// TODO(andy): is this ever 0?
	return 1
}

func (t JSON) isPrimitive() bool {
	return false
}

// Less implements the LesserValuable interface.
func (t JSON) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if _, ok := other.(JSON); !ok {
		return JSONKind < other.Kind(), nil
	}
	h1, err := t.Hash(nbf)
	if err != nil {
		return false, err
	}
	h2, err := other.(JSON).Hash(nbf)
	if err != nil {
		return false, err
	}
	return h1.Compare(h2) < 0, nil
}

func (t JSON) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (t JSON) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (t JSON) HumanReadableString() string {
	h, err := t.Hash(t.nbf)
	if err != nil {
		d.PanicIfError(err)
	}
	return fmt.Sprintf("JSON(%s)", h.String())
}

// UnescapeHTMLCodepoints replaces escaped HTML characters in serialized JSON with their unescaped equivalents.
// Due to an oversight, the representation of JSON in storage escapes these characters, and we unescape them
// before displaying them to the user.
func UnescapeHTMLCodepoints(path []byte) []byte {
	nextToRead := path
	nextToWrite := path

	matches := 0
	index := findNextEscapedUnicodeCodepoint(nextToRead)
	for index != -1 {
		newChar := byte(0)
		if slices.Equal(nextToRead[index+2:index+6], []byte{'0', '0', '3', 'c'}) {
			newChar = '<'
		} else if slices.Equal(nextToRead[index+2:index+6], []byte{'0', '0', '3', 'e'}) {
			newChar = '>'
		} else if slices.Equal(nextToRead[index+2:index+6], []byte{'0', '0', '2', '6'}) {
			newChar = '&'
		}
		if newChar != 0 {
			matches += 1
			copy(nextToWrite, nextToRead[:index])
			nextToWrite[index] = newChar
			nextToWrite = nextToWrite[index+1:]
		}
		nextToRead = nextToRead[index+6:]
		index = findNextEscapedUnicodeCodepoint(nextToRead)
	}
	copy(nextToWrite, nextToRead)
	return path[:len(path)-5*matches]
}

func findNextEscapedUnicodeCodepoint(path []byte) int {
	index := 0
	for {
		if index >= len(path) {
			return -1
		}
		if path[index] == '\\' {
			if path[index+1] == 'u' {
				return index
			}
			index++
		}
		index++
	}
}
