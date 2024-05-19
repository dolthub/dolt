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
	"github.com/mohae/uvarint"
	"io"
)

// TODO: JsonScanner currently assumes that there are no escaped characters in strings. This can be fixed by escaping/unescaping
// strings as they are read from the document, before they are used in any comparisons.

// JsonScanner is a state machine that parses already-normalized JSON while keeping track of the path to the current value.
// It is not a general-purpose JSON parser. In particular, it makes the following assumptions about the JSON:
// - All whitespace has been removed
// - All object keys appear in lexographic order
//
// The purpose of the JsonScanner is to divide a JSON document into segments, where each valid jsonLocation marks a segment boundary.
//
// Each time that AdvanceToNextLocation is called, the scanner advances to the next valid jsonLocation, and the other methods
// provide information about the just-parsed segment.
// |currentPath| will contain the path to the current location.
// |valueOffset| will contain the offset into |jsonBuffer| containing the next byte to be parsed.
// |previousValueOffset| contains the offset from the previous invocation, which is also the start of the last-parsed segment.
// |firstElement| Whether an insertion at the beginning at the segment would be the first element of an object or array.
// (This is necessary for determining whether to generate an extra comma.)
type JsonScanner struct {
	jsonBuffer          []byte
	currentPath         jsonLocation
	valueOffset         int
	previousValueOffset int
	firstElement        bool
}

// ScanJsonFromBeginning creates a new JsonScanner to parse the provided JSON document
func ScanJsonFromBeginning(buf []byte) JsonScanner {
	return JsonScanner{
		jsonBuffer:          buf,
		currentPath:         newRootLocation(),
		valueOffset:         0,
		previousValueOffset: 0,
	}
}

// ScanJsonFromMiddleWithKey creates a new JsonScanner to parse the provided JSON fragment. |pathBytes| is the path at the
// start of the fragment, represented as the key to a StaticJsonMap
func ScanJsonFromMiddleWithKey(buf []byte, pathBytes jsonLocationKey) JsonScanner {
	if pathBytes == nil {
		return ScanJsonFromBeginning(buf)
	}
	path := jsonPathFromKey(pathBytes)
	return ScanJsonFromMiddle(buf, path)
}

// ScanJsonFromMiddle creates a new JsonScanner to parse the provided JSON fragment. |path| is the path at the
// start of the fragment, represented as a jsonLocation
func ScanJsonFromMiddle(buf []byte, path jsonLocation) JsonScanner {
	return JsonScanner{
		jsonBuffer:          buf,
		currentPath:         path,
		valueOffset:         0,
		previousValueOffset: 0,
	}
}

func (s JsonScanner) isParsingArray() bool {
	_, isArray := s.currentPath.getLastPathElement()
	return isArray
}

func (s JsonScanner) firstElementOrEndOfEmptyValue() bool {
	return s.firstElement
}

func (s JsonScanner) atEndOfChunk() bool {
	return s.valueOffset >= len(s.jsonBuffer)
}

func (s JsonScanner) atStartOfValue() bool {
	return s.currentPath.getScannerState() == startOfValue
}

func (s JsonScanner) atEndOfValue() bool {
	return s.currentPath.getScannerState() == endOfValue
}

// skipBytes advances the scanner some number of bytes without parsing. This is used by JsonChunker to write JSON whose
// structure is already known.
func (s *JsonScanner) skipBytes(i int) {
	s.valueOffset += i
}

func (s *JsonScanner) AdvanceToNextLocation() error {
	s.firstElement = false
	s.previousValueOffset = s.valueOffset
	if s.atEndOfChunk() {
		return io.EOF
	}
	if s.atStartOfValue() {
		s.acceptValue()
		return nil
	}
	encodedIndex, isArray := s.currentPath.getLastPathElement()
	s.currentPath.pop()
	if isArray {
		arrayIndex, _ := uvarint.Uvarint(encodedIndex)
		s.acceptNextArrayValue(arrayIndex + 1)
	} else {
		s.acceptNextKeyValue()
	}
	return nil
}

func (s *JsonScanner) acceptValue() {
	current := s.current()
	switch current {
	case '"':
		s.acceptString()
		s.currentPath.setScannerState(endOfValue)
		return
	case '[':
		s.valueOffset++
		s.firstElement = true
		s.acceptFirstArrayValue()
		return
	case '{':
		s.valueOffset++
		s.firstElement = true
		s.acceptKeyValue()
		return
	}
	// The scanner doesn't understand numbers, but it doesn't have to, since the number will be followed by a special character.
	// Thus, we simply scan until we reach a character the scanner understands, marking the end of the value (or the end of the doc).
	s.valueOffset++
	for {
		current = s.current()
		switch s.current() {
		case '}', ']', ',', 255:
			s.currentPath.setScannerState(endOfValue)
			return
		default:
			s.valueOffset++
		}
	}
}

const endOfFile byte = 0xFF

// current returns the current byte being parsed, or 0xFF if we've reached the end of the file.
// (Since the JSON is UTF-8, the 0xFF byte cannot otherwise appear within in.)
func (s JsonScanner) current() byte {
	if s.valueOffset >= len(s.jsonBuffer) {
		return endOfFile
	}
	return s.jsonBuffer[s.valueOffset]
}

func (s *JsonScanner) accept(b byte) {
	current := s.current()
	if current != b {
		s.impossiblePanic()
	}
	s.valueOffset++
}

func (s *JsonScanner) acceptString() []byte {
	s.accept('"')
	stringStart := s.valueOffset
	for s.current() != '"' {
		switch s.current() {
		case '\\':
			s.valueOffset++
		}
		s.valueOffset++
	}
	result := s.jsonBuffer[stringStart:s.valueOffset]
	s.valueOffset++
	return result
}

func (s *JsonScanner) acceptKeyValue() {
	current := s.current()
	switch current {
	case '"':
		s.acceptObjectKey()
	case '}':
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
	default:
		s.impossiblePanic()
	}
}

func (s *JsonScanner) acceptNextKeyValue() {
	current := s.current()
	switch current {
	case ',':
		s.valueOffset++
		if s.current() != '"' {
			s.impossiblePanic()
		}
		s.acceptObjectKey()
	case '}':
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
	default:
		s.impossiblePanic()
	}
}

func (s *JsonScanner) acceptObjectKey() {
	s.currentPath.appendObjectKey(s.acceptString())
	s.accept(':')
	s.currentPath.setScannerState(startOfValue)
}

func (s *JsonScanner) acceptFirstArrayValue() {
	if s.current() == ']' {
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
		return
	}
	s.valueOffset++
	s.currentPath.appendArrayIndex(0)
}

func (s *JsonScanner) acceptNextArrayValue(i uint64) {
	current := s.current()
	switch current {
	case ',':
		s.valueOffset++
		s.currentPath.appendArrayIndex(i)
		s.currentPath.setScannerState(startOfValue)
	case ']':
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
	default:
		s.impossiblePanic()
	}
}

func (JsonScanner) impossiblePanic() {
	panic("Invalid JSON in JsonScanner. This is impossible.")
}
