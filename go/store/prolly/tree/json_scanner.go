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
	"fmt"
	"io"
)

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
// |firstElement| Whether an insertion at the beginning at the segment would be the first element of an object or array.
// (This is necessary for determining whether to generate an extra comma.)
type JsonScanner struct {
	jsonBuffer  []byte
	currentPath jsonLocation
	valueOffset int
}

var jsonParseError = fmt.Errorf("encountered invalid JSON while reading JSON from the database, or while preparing to write JSON to the database. This is most likely a bug in JSON diffing")

func (j JsonScanner) Clone() JsonScanner {
	return JsonScanner{
		jsonBuffer:  j.jsonBuffer,
		currentPath: j.currentPath.Clone(),
		valueOffset: j.valueOffset,
	}
}

// ScanJsonFromBeginning creates a new JsonScanner to parse the provided JSON document
func ScanJsonFromBeginning(buf []byte) JsonScanner {
	return JsonScanner{
		jsonBuffer:  buf,
		currentPath: newRootLocation(),
		valueOffset: 0,
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
		jsonBuffer:  buf,
		currentPath: path,
		valueOffset: 0,
	}
}

func (s JsonScanner) isParsingArray() bool {
	return s.currentPath.getLastPathElement().isArrayIndex
}

func (s JsonScanner) firstElementOrEndOfEmptyValue() bool {
	return s.currentPath.getScannerState() == objectInitialElement || s.currentPath.getScannerState() == arrayInitialElement
}

func (s JsonScanner) atEndOfChunk() bool {
	return s.valueOffset >= len(s.jsonBuffer)
}

func (s JsonScanner) atStartOfValue() bool {
	return s.currentPath.getScannerState() == startOfValue
}

// skipBytes advances the scanner some number of bytes without parsing. This is used by JsonChunker to write JSON whose
// structure is already known.
func (s *JsonScanner) skipBytes(i int) {
	s.valueOffset += i
}

func (s *JsonScanner) AdvanceToNextLocation() error {
	if s.atEndOfChunk() {
		return io.EOF
	}
	switch s.currentPath.getScannerState() {
	case startOfValue:
		return s.acceptValue()
	case objectInitialElement:
		return s.acceptKeyValue()
	case arrayInitialElement:
		return s.acceptFirstArrayValue()
	case endOfValue:
		lastPathElement := s.currentPath.getLastPathElement()
		s.currentPath.pop()
		if lastPathElement.isArrayIndex {
			return s.acceptNextArrayValue(lastPathElement.getArrayIndex() + 1)
		} else {
			return s.acceptNextKeyValue()
		}
	default:
		return jsonParseError
	}
}

func (s *JsonScanner) acceptValue() error {
	current := s.current()
	switch current {
	case '"':
		_, err := s.acceptString()
		if err != nil {
			return err
		}
		s.currentPath.setScannerState(endOfValue)
		return nil
	case '[':
		s.valueOffset++
		s.currentPath.setScannerState(arrayInitialElement)
		return nil
	case '{':
		s.valueOffset++
		s.currentPath.setScannerState(objectInitialElement)
		return nil
	}
	// The scanner doesn't understand numbers, but it doesn't have to, since the number will be followed by a special character.
	// Thus, we simply scan until we reach a character the scanner understands, marking the end of the value (or the end of the doc).
	s.valueOffset++
	for {
		current = s.current()
		switch current {
		case '}', ']', ',', endOfFile:
			s.currentPath.setScannerState(endOfValue)
			return nil
		default:
			s.valueOffset++
		}
	}
}

const endOfFile byte = 0xFF

// current returns the current byte being parsed, or 0xFF if we've reached the end of the file.
// (Since the JSON is UTF-8, the 0xFF byte cannot otherwise appear within it.)
func (s JsonScanner) current() byte {
	if s.valueOffset >= len(s.jsonBuffer) {
		return endOfFile
	}
	return s.jsonBuffer[s.valueOffset]
}

func (s *JsonScanner) accept(b byte) error {
	current := s.current()
	if current != b {
		return jsonParseError
	}
	s.valueOffset++
	return nil
}

func (s *JsonScanner) acceptString() ([]byte, error) {
	err := s.accept('"')
	if err != nil {
		return nil, err
	}
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
	return result, nil
}

func (s *JsonScanner) acceptKeyValue() error {
	current := s.current()
	switch current {
	case '"':
		return s.acceptObjectKey()
	case '}':
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
		return nil
	default:
		return jsonParseError
	}
}

func (s *JsonScanner) acceptNextKeyValue() error {
	current := s.current()
	switch current {
	case ',':
		s.valueOffset++
		if s.current() != '"' {
			return jsonParseError
		}
		return s.acceptObjectKey()
	case '}':
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
		return nil
	default:
		return jsonParseError
	}
}

func (s *JsonScanner) acceptObjectKey() error {
	objectKey, err := s.acceptString()
	if err != nil {
		return err
	}
	s.currentPath.appendObjectKey(unescapeKey(objectKey))
	err = s.accept(':')
	if err != nil {
		return err
	}
	s.currentPath.setScannerState(startOfValue)
	return nil
}

func (s *JsonScanner) acceptFirstArrayValue() error {
	if s.current() == ']' {
		s.valueOffset++
		s.currentPath.setScannerState(endOfValue)
		return nil
	}
	s.currentPath.setScannerState(startOfValue)
	s.currentPath.appendArrayIndex(0)
	return nil
}

func (s *JsonScanner) acceptNextArrayValue(i uint64) error {
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
		return jsonParseError
	}
	return nil
}
