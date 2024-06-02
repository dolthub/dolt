// Copyright 2024 Dolthub, Inc.
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

package tree

import (
	"bytes"
	"cmp"
	"fmt"
	"github.com/mohae/uvarint"
	"slices"
	"strconv"
)

// jsonLocation is a representation of a path into a JSON document. It is designed for efficient in-place modification and fast
// comparisons. The |offsets| field is redundant and can be generated from the |key| field using the jsonPathFromKey function.
//
// Every jsonLocation points to a specific byte offset in the JSON document, either the start or end of a value. This allows for
// comparisons between paths that may refer to parent and child objects. For example:
//
// "start of $.a" < "start of $.a.b" < "end of $.a.b" < "end of $.a" < "start of $.c" < "end of $.c" < "end of $"
//
// |key| - The first byte is a scannerState which indicates whether this path marks the start of end of a value.
//
//			The remainder of |key| is a sequence of encoded path elements, each of which is either an object key or array index:
//			    <path-element> ::= <object-key> | <array-index>
//		        <object-key>   ::= 0xFF <UTF-8 encoded key>
//		        <array-index>  ::= 0xFE <SQLite4 varint> (https://sqlite.org/src4/doc/trunk/www/varint.wiki)
//	     SQLite4 varint encoding was chosen for the array index because it has the property that a lexographic ordering of
//	     encoded values preserves order (if a < b, then encode(a) < encode(b)).
//	     The bytes 0xFF and 0xFE were chosen as separators because they are the only bytes which are not valid UTF-8,
//	     and thus there is no need to escape any characters in the encoded object key. While these bytes can appear in
//	     a SQLite4 varint, the length of the varint can be determined by its first byte, so there's no ambiguity.
//
// |offsets| - This field stores an offset to the start of each path element in |key|, plus an offset to the end of |key|
type jsonLocation struct {
	key     []byte
	offsets []int
}

type scannerState byte

const (
	startOfValue scannerState = 0xFE
	endOfValue   scannerState = 0xFF
)

const (
	beginObjectKey byte = 0xFF
	beginArrayKey  byte = 0xFE
)

const VarintCacheSize = 8

// Precompute and cache the first few varints, since these will be used the most.
var varInts = func() [][]byte {
	v := make([][]byte, VarintCacheSize)
	for i, _ := range v {
		v[i] = makeVarInt(uint64(i))
	}
	return v
}()

func makeVarInt(x uint64) (v []byte) {
	v = make([]byte, 9)
	length := uvarint.Encode(v, x)
	return v[:length]
}

func getVarInt(x uint64) []byte {
	if x < VarintCacheSize {
		return varInts[x]
	}
	return makeVarInt(x)
}

func newRootLocation() jsonLocation {
	return jsonLocation{
		key:     []byte{byte(startOfValue)},
		offsets: []int{1},
	}
}

// jsonPathFromKey creates a jsonLocation from a StaticJsonMap key.
func jsonPathFromKey(pathKey []byte) (path jsonLocation) {
	path = newRootLocation()
	path.setScannerState(scannerState(pathKey[0]))
	if len(pathKey) == 1 {
		return path
	}
	startIdx := 1
	maxIdx := len(pathKey)
	for {
		if startIdx >= maxIdx {
			return path
		}
		separatorByte := pathKey[startIdx]
		startIdx++
		switch separatorByte {
		case beginObjectKey:
			// scan until we encounter the next separator byte or the end of the key
			endIdx := startIdx
			for {
				if endIdx >= maxIdx {
					path.appendObjectKey(pathKey[startIdx:])
					return path
				}
				current := pathKey[endIdx]
				if current == beginArrayKey || current == beginObjectKey {
					break
				}
				endIdx++
			}
			path.appendObjectKey(pathKey[startIdx:endIdx])
			startIdx = endIdx
			continue
		case beginArrayKey:
			size := varIntLength(pathKey[startIdx])
			path.appendEncodedArrayIndex(pathKey[startIdx : startIdx+size])
			startIdx += size
		default:
			panic(fmt.Sprintf("invalid varint in json path key %v. This is either a bug or database corruption.", pathKey))
		}
	}
}

// varIntLength returns the length of a SQLite4 varint in bytes, given the contents of the first byte.
// (https://sqlite.org/src4/doc/trunk/www/varint.wiki)
func varIntLength(firstByte byte) int {
	if firstByte <= 240 {
		return 1
	}
	if firstByte <= 248 {
		return 2
	}
	return int(firstByte - 246)
}

func isValidJsonPathKey(key []byte) bool {
	if bytes.Equal(key, []byte("*")) {
		return false
	}
	if bytes.Equal(key, []byte("**")) {
		return false
	}
	return true
}

// jsonPathElementsFromMySQLJsonPath computes a jsonLocation from a MySQL JSON path (https://dev.mysql.com/doc/refman/8.0/en/json.html#json-path-syntax)
func jsonPathElementsFromMySQLJsonPath(pathBytes []byte) (path jsonLocation, err error) {
	path = newRootLocation()
	originalPathBytes := pathBytes
	// TODO: We need to account for escaped characters
	if pathBytes[0] != '$' {
		return path, fmt.Errorf("Invalid JSON path expression. Path must start with '$'")
	}
	parsedCharacters := 1
	pathBytes = pathBytes[1:]
	validateAndAppendObjectKeyToPath := func(key []byte) error {
		if !isValidJsonPathKey(key) {
			return fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character %v of %s", parsedCharacters+1, originalPathBytes)
		}
		path.appendObjectKey(key)
		return nil
	}
	for len(pathBytes) > 0 {
		var endOffset int
		if pathBytes[0] == '[' {
			endOffset = bytes.IndexByte(pathBytes, ']')
			arrayIndex, err := strconv.Atoi(string(pathBytes[1:endOffset]))
			if err != nil {
				return path, err
			}
			path.appendArrayIndex(uint64(arrayIndex))
			pathBytes = pathBytes[endOffset+1:]
			parsedCharacters += endOffset + 1
		} else if pathBytes[0] == '.' {
			// TODO: Also accept a double-quoted string.
			pathBytes = pathBytes[1:]
			nextIndex := bytes.IndexByte(pathBytes, '[')
			nextKey := bytes.IndexByte(pathBytes, '.')
			endOffset = nextIndex
			if nextIndex == -1 || (nextKey != -1 && nextKey < nextIndex) {
				endOffset = nextKey
			}
			if endOffset == -1 {
				err = validateAndAppendObjectKeyToPath(pathBytes)
				return path, err
			}
			if endOffset == 0 {
				// Unquoted empty key is not allowed.
				return path, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character %v of %s", parsedCharacters+1, originalPathBytes)
			}
			err = validateAndAppendObjectKeyToPath(pathBytes[:endOffset])
			if err != nil {
				return path, err
			}
			pathBytes = pathBytes[endOffset:]
			parsedCharacters += endOffset + 1
		} else {
			return path, fmt.Errorf("Invalid JSON path expression '%s'", originalPathBytes)
		}
	}
	return path, nil
}

func (p *jsonLocation) appendObjectKey(key []byte) {
	p.key = append(append(p.key, beginObjectKey), key...)
	p.offsets = append(p.offsets, len(p.key))
}

func (p *jsonLocation) appendArrayIndex(idx uint64) {
	p.key = append(append(p.key, beginArrayKey), getVarInt(idx)...)
	p.offsets = append(p.offsets, len(p.key))
}

func (p *jsonLocation) appendEncodedArrayIndex(idx []byte) {
	p.key = append(append(p.key, beginArrayKey), idx...)
	p.offsets = append(p.offsets, len(p.key))
}

func (p *jsonLocation) pop() {
	lastOffset := p.offsets[len(p.offsets)-2]
	p.offsets = p.offsets[:len(p.offsets)-1]
	p.key = p.key[:lastOffset]
}

func (p *jsonLocation) setScannerState(s scannerState) {
	p.key[0] = byte(s)
}

func (p *jsonLocation) getScannerState() scannerState {
	return scannerState(p.key[0])
}

func (p *jsonLocation) getPathElement(i int) (key []byte, isArray bool) {
	start := p.offsets[i]
	end := p.offsets[i+1]
	key = p.key[start+1 : end]
	isArray = p.key[start] == beginArrayKey
	return key, isArray
}

func (p *jsonLocation) size() int {
	return len(p.offsets) - 1
}

func (p *jsonLocation) getLastPathElement() (key []byte, isArray bool) {
	return p.getPathElement(p.size() - 1)
}

func (p *jsonLocation) Clone() jsonLocation {
	return jsonLocation{
		key:     bytes.Clone(p.key),
		offsets: slices.Clone(p.offsets),
	}
}

// compareJsonLocations creates an ordering on locations by determining which one would come first in a normalized JSON
// document where all keys are sorted lexographically.
func compareJsonLocations(left, right jsonLocation) int {
	minLength := min(left.size(), right.size())
	for i := 0; i < minLength; i++ {
		l, _ := left.getPathElement(i)
		r, _ := right.getPathElement(i)
		c := bytes.Compare(l, r)
		if c < 0 {
			return -1
		}
		if c > 0 {
			return 1
		}
	}
	if left.size() < right.size() {
		// left is a parent of right
		if left.getScannerState() == startOfValue {
			return -1
		}
		return 1
	}
	if left.size() > right.size() {
		// right is a parent of left
		if right.getScannerState() == startOfValue {
			return 1
		}
		return -1
	}
	// left and right have the exact same key elements
	return cmp.Compare(left.getScannerState(), right.getScannerState())

}

type jsonLocationOrdering struct{}

var _ Ordering[[]byte] = jsonLocationOrdering{}

func (jsonLocationOrdering) Compare(left, right []byte) int {
	leftPath := jsonPathFromKey(left)
	rightPath := jsonPathFromKey(right)
	return compareJsonLocations(leftPath, rightPath)
}
