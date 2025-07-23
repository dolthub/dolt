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
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/mohae/uvarint"
)

// jsonLocation is a representation of a path into a JSON document. It is designed for efficient in-place modification and fast
// comparisons. The |offsets| field is redundant and can be generated from the |key| field using the jsonPathFromKey function.
//
// Every jsonLocation references a specific named value within the document, and points to a specific byte offset. This allows for
// comparisons between paths that may refer to parent and child objects. For example:
//
// "start of $.a" < "start of $.a.b" < "end of $.a.b" < "end of $.a" < "start of $.c" < "end of $.c" < "end of $"
//
// |key| - The first byte is a jsonPathType which indicates what part of the named object is being represented.
//
//	        The possible values for this byte include:
//	             - 0 / startOfValue         - The location points to the first character of the value.
//	             - 1 / objectInitialElement - The location points to where a new value would be inserted at the start of an object.
//	                                          This is always one byte past the start of the object.
//	             - 2 / arrayInitialElement  - The location points to where a new value would be inserted at the start of an array.
//	                                          This is always one byte past the start of the array.
//	             - 3 / startOfValue         - The location points one byte past the end of the value.
//	        In the event this format changes, the upper bits can be used to indicate the version. Currently, if these bits are not
//	        all 0, the engine should emit a warning, and then fall back on a naive implementation of the current operation that
//	        does not require these keys.
//
//				The remainder of |key| is a sequence of encoded path elements, each of which is either an object key or array index:
//				    <path-element> ::= <object-key> | <array-index>
//			        <object-key>   ::= 0xFF <UTF-8 encoded key>
//			        <array-index>  ::= 0xFE <SQLite4 varint> (https://sqlite.org/src4/doc/trunk/www/varint.wiki)
//		     SQLite4 varint encoding was chosen for the array index because it has the property that a lexographic ordering of
//		     encoded values preserves order (if a < b, then encode(a) < encode(b)).
//		     The bytes 0xFF and 0xFE were chosen as separators because they are the only bytes which are not valid UTF-8,
//		     and thus there is no need to escape any characters in the encoded object key. While these bytes can appear in
//		     a SQLite4 varint, the length of the varint can be determined by its first byte, so there's no ambiguity.
//
// |offsets| - This field stores an offset to the start of each path element in |key|, plus an offset to the end of |key|
type jsonLocation struct {
	key     []byte
	offsets []int
}

// Every
type jsonPathType byte

const (
	startOfValue jsonPathType = iota
	objectInitialElement
	arrayInitialElement
	endOfValue
	middleOfStringValue
	jsonPathTypeNumElements
)

func compareJsonPathTypes(left, right jsonPathType) (int, error) {
	if left >= jsonPathTypeNumElements || right >= jsonPathTypeNumElements {
		// These paths were written by a future version of Dolt. We can't assume anything about them.
		return 0, unknownLocationKeyError
	}
	if left == startOfValue && right != startOfValue {
		return -1, nil
	}
	if left == endOfValue && right != endOfValue {
		return 1, nil
	}
	if right == startOfValue && left != startOfValue {
		return 1, nil
	}
	if right == endOfValue && left != endOfValue {
		return -1, nil
	}
	return 0, nil
}

func (t jsonPathType) isInitialElement() bool {
	return t == objectInitialElement || t == arrayInitialElement
}

var unknownLocationKeyError = fmt.Errorf("A JSON document was written with a future version of Dolt, and the index metadata cannot be read. This will impact performance for large documents.")
var unsupportedPathError = fmt.Errorf("The supplied JSON path is not supported for optimized lookup, falling back on unoptimized implementation.")

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
	ret := jsonLocation{
		key:     bytes.Clone(pathKey),
		offsets: []int{},
	}
	i := 1
	for i < len(pathKey) {
		if pathKey[i] == beginObjectKey {
			ret.offsets = append(ret.offsets, i)
			i += 1
		} else if pathKey[i] == beginArrayKey {
			ret.offsets = append(ret.offsets, i)
			i += 1
			i += varIntLength(pathKey[i])
		} else {
			i += 1
		}
	}
	ret.offsets = append(ret.offsets, i)
	return ret
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

func isUnsupportedJsonPathKey(key []byte) bool {
	if bytes.Equal(key, []byte("*")) {
		return true
	}
	if bytes.Equal(key, []byte("**")) {
		return true
	}
	return false
}

func isUnsupportedJsonArrayIndex(index []byte) bool {
	if bytes.Equal(index, []byte("*")) {
		return true
	}
	if bytes.Equal(index, []byte("last")) {
		return true
	}
	return false
}

func errorIfNotSupportedLocation(key []byte) error {
	if jsonPathType(key[0]) > jsonPathTypeNumElements {
		return unknownLocationKeyError
	}
	return nil
}

type lexState int

const (
	lexStatePath             lexState = 1
	lexStateIdx              lexState = 2
	lexStateKey              lexState = 3
	lexStateQuotedKey        lexState = 4
	lexStateEscapedQuotedKey lexState = 5
)

func escapeKey(key []byte) []byte {
	return bytes.Replace(key, []byte(`"`), []byte(`\"`), -1)
}

func unescapeKey(key []byte) []byte {
	return bytes.Replace(key, []byte(`\"`), []byte(`"`), -1)
}

// IsJsonKeyPrefix computes whether one key encodes a json location that is a prefix of another.
// Example: $.a is a prefix of $.a.b, but not $.aa
func IsJsonKeyPrefix(path, prefix []byte) bool {
	return bytes.HasPrefix(path, prefix) && (path[len(prefix)] == beginArrayKey || path[len(prefix)] == beginObjectKey)
}

func JsonKeysModifySameArray(leftKey, rightKey []byte) bool {
	i := 0
	for i < len(leftKey) && i < len(rightKey) && leftKey[i] == rightKey[i] {
		if leftKey[i] == beginArrayKey {
			return true
		}
		i++
	}
	return false
}

func jsonPathElementsFromMySQLJsonPath(pathBytes []byte) (jsonLocation, error) {
	location := newRootLocation()
	state := lexStatePath
	// Start of the token.
	tok := 0
	// Current index into |pathBytes|.
	if len(pathBytes) == 0 || pathBytes[0] != '$' {
		return jsonLocation{}, fmt.Errorf("Invalid JSON path expression. Path must start with '$', but received: '%s'", pathBytes)
	}
	i := 1
	for i < len(pathBytes) {
		switch state {
		case lexStatePath:
			if pathBytes[i] == '[' {
				i += 1
				tok = i
				state = lexStateIdx
			} else if pathBytes[i] == '.' {
				i += 1
				tok = i
				state = lexStateKey
			} else {
				return jsonLocation{}, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character %v of %s", i, string(pathBytes))
			}
		case lexStateIdx:
			if pathBytes[i] != byte(']') {
				i += 1
			} else {
				indexBytes := pathBytes[tok:i]
				if isUnsupportedJsonArrayIndex(indexBytes) {
					return jsonLocation{}, unsupportedPathError
				}
				conv, err := strconv.Atoi(string(pathBytes[tok:i]))
				if err != nil {
					return jsonLocation{}, fmt.Errorf("Invalid JSON path expression. Expected array index name after '[' at character %v of %s", i, string(pathBytes))
				}
				i++
				location.appendArrayIndex(uint64(conv))
				state = lexStatePath
			}
		case lexStateKey:
			if pathBytes[i] == '"' {
				tok = i // Point tok to the opening quote
				state = lexStateQuotedKey
				i += 1
			} else if pathBytes[i] == '.' || pathBytes[i] == '[' {
				if tok == i {
					// This could be a .[*] expression. Let the original implementation take a stab at it.
					return jsonLocation{}, unsupportedPathError
				}
				if isUnsupportedJsonPathKey(pathBytes[tok:i]) {
					return jsonLocation{}, unsupportedPathError
				}
				location.appendObjectKey(pathBytes[tok:i])
				state = lexStatePath
			} else {
				i += 1
			}
		case lexStateQuotedKey:
			if pathBytes[i] == '"' {
				if tok+1 == i {
					return jsonLocation{}, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character %v of %s", i, string(pathBytes))
				}
				pathKey := unescapeKey(pathBytes[tok+1 : i])
				if isUnsupportedJsonPathKey(pathKey) {
					return jsonLocation{}, unsupportedPathError
				}
				location.appendObjectKey(pathKey)
				state = lexStatePath
				i += 1
			} else if pathBytes[i] == '\\' {
				state = lexStateEscapedQuotedKey
				i += 1
			} else {
				i += 1
			}
		case lexStateEscapedQuotedKey:
			state = lexStateQuotedKey
			i += 1
		}
	}
	if state == lexStateKey {
		if tok == i {
			return jsonLocation{}, fmt.Errorf("Invalid JSON path expression. Expected field name after '.' at character %v of %s", i, string(pathBytes))
		}
		if isUnsupportedJsonPathKey(pathBytes[tok:i]) {
			return jsonLocation{}, unsupportedPathError
		}
		location.appendObjectKey(pathBytes[tok:i])
		state = lexStatePath
	}
	if state != lexStatePath {
		return jsonLocation{}, fmt.Errorf("invalid JSON path expression %s", string(pathBytes))
	}
	return location, nil
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

func (p *jsonLocation) setScannerState(s jsonPathType) {
	p.key[0] = byte(s)
}

func (p *jsonLocation) getScannerState() jsonPathType {
	return jsonPathType(p.key[0])
}

func (p jsonLocation) IsMiddleOfString() bool {
	return p.getScannerState() == middleOfStringValue
}

type jsonPathElement struct {
	key          []byte
	isArrayIndex bool
}

func (e jsonPathElement) getArrayIndex() uint64 {
	arrayIndex, _ := uvarint.Uvarint(e.key)
	return arrayIndex
}

func (p *jsonLocation) getPathElement(i int) (result jsonPathElement) {
	start := p.offsets[i]
	end := p.offsets[i+1]
	result.key = p.key[start+1 : end]
	result.isArrayIndex = p.key[start] == beginArrayKey
	return result
}

func (p *jsonLocation) size() int {
	return len(p.offsets) - 1
}

func (p *jsonLocation) getLastPathElement() (result jsonPathElement) {
	state := p.getScannerState()
	if state == arrayInitialElement {
		return jsonPathElement{nil, true}
	}
	if state == objectInitialElement {
		return jsonPathElement{nil, false}
	}
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
func compareJsonLocations(left, right jsonLocation) (int, error) {
	minLength := min(left.size(), right.size())
	for i := 0; i < minLength; i++ {
		leftPathElement := left.getPathElement(i)
		rightPathElement := right.getPathElement(i)
		c := bytes.Compare(leftPathElement.key, rightPathElement.key)
		if c < 0 {
			return -1, nil
		}
		if c > 0 {
			return 1, nil
		}
	}
	if left.size() < right.size() {
		// left is a parent of right
		// there's a special case here: the user may be trying to treat a scalar or object as an array, which causes
		// it to get implicitly wrapped in an array. In order for the cursor to return the correct location for
		// reading/modification, for any object b, b[N] must compare less than the initial element location of b.
		if right.size() == left.size()+1 {
			if left.getScannerState() == objectInitialElement {
				rightIsArray := right.getLastPathElement().isArrayIndex
				if rightIsArray {
					return 1, nil
				}
			}
		}
		if left.getScannerState() != endOfValue {
			return -1, nil
		}
		return 1, nil
	}
	if left.size() > right.size() {
		// right is a parent of left
		// there's a special case here: the user may be trying to treat a scalar or object as an array, which causes
		// it to get implicitly wrapped in an array. In order for the cursor to return the correct location for
		// reading/modification, for any object b, b[N] must compare less than the initial element location of b.
		if left.size() == right.size()+1 {
			if right.getScannerState() == objectInitialElement {
				leftIsArray := left.getLastPathElement().isArrayIndex
				if leftIsArray {
					return -1, nil
				}
			}
		}

		if right.getScannerState() != endOfValue {
			return 1, nil
		}
		return -1, nil
	}
	// left and right have the exact same key elements
	return compareJsonPathTypes(left.getScannerState(), right.getScannerState())

}

type jsonLocationOrdering struct {
	err error // store errors created during comparisons
}

var _ Ordering[[]byte] = &jsonLocationOrdering{}

func (o *jsonLocationOrdering) Compare(ctx context.Context, left, right []byte) int {
	// A JSON document that fits entirely in a single chunk has no keys,
	if len(left) == 0 && len(right) == 0 {
		return 0
	} else if len(left) == 0 {
		return -1
	} else if len(right) == 0 {
		return 1
	}
	leftPath := jsonPathFromKey(left)
	rightPath := jsonPathFromKey(right)
	cmp, err := compareJsonLocations(leftPath, rightPath)
	if err != nil {
		o.err = err
	}
	return cmp
}
