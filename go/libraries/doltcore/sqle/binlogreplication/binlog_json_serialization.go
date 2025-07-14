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

package binlogreplication

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
)

const jsonTypeSmallObject = byte(0x00)
const jsonTypeLargeObject = byte(0x01)
const jsonTypeSmallArray = byte(0x02)
const jsonTypeLargeArray = byte(0x03)
const jsonTypeLiteral = byte(0x04)
const jsonTypeInt16 = byte(0x05)
const jsonTypeUint16 = byte(0x06)
const jsonTypeInt32 = byte(0x07)
const jsonTypeUint32 = byte(0x08)
const jsonTypeInt64 = byte(0x09)
const jsonTypeUint64 = byte(0x0a)
const jsonTypeDouble = byte(0x0b)
const jsonTypeString = byte(0x0c)
const jsonTypeCustom = byte(0x0f)

const jsonLiteralValueNull = byte(0x00)
const jsonLiteralValueTrue = byte(0x01)
const jsonLiteralValueFalse = byte(0x02)

// maxOffsetSize is used to determine if an byte offset into an array or object encoding will exceed the capacity
// of a uint16 and whether the encoding needs to switch to the large array or large object format.
const maxOffsetSize = uint32(65_535)

// encodeJsonDoc encodes the specified |jsonDoc| into MySQL's custom/internal binary encoding
// so that it can be included in a binlog event.
//
// The internal MySQL JSON binary format is documented here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/json__binary_8h.html
//
// And a third-party description is here:
// https://lafengnan.gitbooks.io/blog/content/mysql/chapter2.html
func encodeJsonDoc(ctx context.Context, jsonDoc sql.JSONWrapper) (buffer []byte, err error) {
	val, err := jsonDoc.ToInterface(ctx)
	if err != nil {
		return nil, err
	}
	typeId, encodedValue, err := encodeJsonValue(val)
	if err != nil {
		return nil, err
	}
	buffer = append(buffer, typeId)
	return append(buffer, encodedValue...), nil
}

// encodeJsonArray encodes the specified |jsonArray| into MySQL's internal JSON encoding and returns
// the type ID indicating whether this is a small or large array, the encoded array data, and any
// error encountered. The |largeEncoding| param controls whether this function will use the small
// array encoding (i.e. using 2 bytes for counts, sizes, and offsets), or the large array encoding
// (i.e. using 4 bytes for counts, sizes, and offsets).
//
// A JSON Array is encoded into the following components:
// - Type Identifier: Always 1 byte; jsonTypeSmallArray or jsonTypeLargeArray; not included in the returned []byte.
// - Count:  2 bytes for small encoding, otherwise 4; number of elements in the array
// - Size: 2 bytes for small encoding, otherwise 4; total size of the encoded array (everything but the Type ID)
// - Value Entries: 1 per value; 1 byte for type ID, variable sized offset (or inlined literal value)
// - Values: 1 per value; encoded value bytes
func encodeJsonArray(jsonArray []any, largeEncoding bool) (typeId byte, encodedArray []byte, err error) {
	if !largeEncoding && len(jsonArray) > int(maxOffsetSize) {
		return 0, nil, fmt.Errorf(
			"too many elements in JSON array (%d) to serialize in small array encoding", len(jsonArray))
	}

	var valueEntriesBuffer []byte
	var valuesBuffer []byte
	nextValuesOffset := calculateInitialArrayValuesOffset(len(jsonArray), largeEncoding)

	for _, element := range jsonArray {
		typeId, encodedValue, err := encodeJsonValue(element)
		if err != nil {
			return 0, nil, err
		}

		// Literals can be inlined in the value-entries section
		if typeId == jsonTypeLiteral {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			if len(encodedValue) != 1 {
				return 0, nil, fmt.Errorf("unexpected buffer length")
			}
			valueEntriesBuffer = appendForEncoding(valueEntriesBuffer, uint32(encodedValue[0]), largeEncoding)
		} else {
			if !largeEncoding && nextValuesOffset > maxOffsetSize-uint32(len(encodedValue)) {
				return 0, nil, fmt.Errorf("offset too large for small array encoding")
			}

			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			valueEntriesBuffer = appendForEncoding(valueEntriesBuffer, nextValuesOffset, largeEncoding)
			valuesBuffer = append(valuesBuffer, encodedValue...)
			nextValuesOffset += uint32(len(encodedValue))
		}
	}

	// element count (uint16 for small arrays)
	encodedArray = appendForEncoding(encodedArray, uint32(len(jsonArray)), largeEncoding)

	// Grab the total size of the array data from the next offset position pointing to the end of the values buffer
	arrayPayloadLength := nextValuesOffset

	encodedArray = appendForEncoding(encodedArray, arrayPayloadLength, largeEncoding)
	encodedArray = append(encodedArray, valueEntriesBuffer...)
	encodedArray = append(encodedArray, valuesBuffer...)

	if !largeEncoding {
		return jsonTypeSmallArray, encodedArray, nil
	} else {
		return jsonTypeLargeArray, encodedArray, nil
	}
}

// encodeJsonObject encodes the specified |jsonObject| into MySQL's internal JSON encoding and returns
// the type ID indicating whether this is a small or large object, the encoded object data, and any
// error encountered. The |largeEncoding| param controls whether this function will use the small
// object encoding (i.e. using 2 bytes for counts, sizes, and offsets), or the large object encoding
// (i.e. using 4 bytes for counts, sizes, and offsets).
//
// A JSON Object is encoded into the following components:
// - Type Identifier: Always 1 byte; either jsonTypeSmallObject or jsonTypeLargeObject (not included in returned []byte)
// - Count: variable based on small/large encoding; holds the number of keys in the object
// - Size: variable based on small/large encoding; total size of the encoded object (i.e. everything but the Type ID)
// - Key Entries: 1 per key; variable length key offset (based on small/large encoding), plus 2 bytes for key length
// - Value Entries (variable): 1 per value; 1 byte for type ID, 2 bytes for offset or inlined literal value for jsonTypeSmallObject, otherwise 4
// - Keys (variable): 1 per key; encoded string bytes
// - Values (variable): 1 per value; encoded value bytes
func encodeJsonObject(jsonObject map[string]any, largeEncoding bool) (typeId byte, encodedObject []byte, err error) {
	var keyEntriesBuffer []byte
	var keysBuffer []byte
	nextKeysOffset := calculateInitialObjectKeysOffset(len(jsonObject), largeEncoding)

	// Sort the keys so that we can process the keys and values in a consistent order. MySQL seems to sort
	// json keys internally first by length, then alphabetically, but correct replication doesn't seem to
	// rely on matching that behavior.
	sortedKeys := make([]string, 0, len(jsonObject))
	for key := range jsonObject {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	// Process keys first, since value entry data depends on offsets that we don't know until we
	// process all the keys.
	for _, key := range sortedKeys {
		// NOTE: Don't use encodeJsonValue for the key – its length gets encoded slightly differently
		//       for JSON objects.
		encodedValue := []byte(key)

		if !largeEncoding && nextKeysOffset > maxOffsetSize-uint32(len(encodedValue)) {
			return 0, nil, fmt.Errorf("offset too large for small object encoding")
		}

		keyEntriesBuffer = appendForEncoding(keyEntriesBuffer, nextKeysOffset, largeEncoding)
		keyEntriesBuffer = append(keyEntriesBuffer, byte(len(encodedValue)), byte(len(encodedValue)<<8))
		keysBuffer = append(keysBuffer, encodedValue...)
		nextKeysOffset += uint32(len(encodedValue))
	}

	// Process values – since the object values are written after the keys, and we need to store the
	// offsets to those locations in the value entries that appear before the keys and the values, we
	// have to make a second pass through the object to process the values once we know the final
	// length of the keys section.
	var valueEntriesBuffer []byte
	var valuesBuffer []byte
	nextValuesOffset := nextKeysOffset
	for _, key := range sortedKeys {
		value := jsonObject[key]
		typeId, encodedValue, err := encodeJsonValue(value)
		if err != nil {
			return 0, nil, err
		}

		// Literals may be inlined in the value-entries section
		if typeId == jsonTypeLiteral {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			if len(encodedValue) != 1 {
				return 0, nil, fmt.Errorf("unexpected buffer length")
			}
			valueEntriesBuffer = appendForEncoding(valueEntriesBuffer, uint32(encodedValue[0]), largeEncoding)
		} else {
			if !largeEncoding && nextValuesOffset > maxOffsetSize-uint32(len(encodedValue)) {
				return 0, nil, fmt.Errorf("offset too large for small object encoding")
			}

			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			valueEntriesBuffer = appendForEncoding(valueEntriesBuffer, nextValuesOffset, largeEncoding)
			valuesBuffer = append(valuesBuffer, encodedValue...)
			nextValuesOffset += uint32(len(encodedValue))
		}
	}

	// element count (uint16 for small objects)
	encodedObject = appendForEncoding(encodedObject, uint32(len(jsonObject)), largeEncoding)

	// Grab the total size of the object data from the next offset position pointing to the end of the values buffer
	objectPayloadLength := nextValuesOffset

	encodedObject = appendForEncoding(encodedObject, objectPayloadLength, largeEncoding)
	encodedObject = append(encodedObject, keyEntriesBuffer...)
	encodedObject = append(encodedObject, valueEntriesBuffer...)
	encodedObject = append(encodedObject, keysBuffer...)
	encodedObject = append(encodedObject, valuesBuffer...)

	if !largeEncoding {
		return jsonTypeSmallObject, encodedObject, nil
	} else {
		return jsonTypeLargeObject, encodedObject, nil
	}
}

// encodeJsonObject encodes the specified |jsonValue| into MySQL's internal JSON encoding and returns
// the type ID indicating what type of value this is, the encoded value, and any error encountered.
func encodeJsonValue(jsonValue any) (typeId byte, buffer []byte, err error) {
	if jsonValue == nil {
		buffer = append(buffer, jsonLiteralValueNull)
		return jsonTypeLiteral, buffer, nil
	}

	switch v := jsonValue.(type) {
	case bool:
		if v {
			buffer = append(buffer, jsonLiteralValueTrue)
		} else {
			buffer = append(buffer, jsonLiteralValueFalse)
		}
		return jsonTypeLiteral, buffer, nil

	case string:
		// String lengths use a special encoding that can span multiple bytes
		buffer, err = appendStringLength(buffer, len(v))
		if err != nil {
			return 0, nil, err
		}

		buffer = append(buffer, []byte(v)...)
		return jsonTypeString, buffer, nil

	case float64:
		// NOTE: all our numbers end up being represented as float64s currently when we parse stored JSON
		bits := math.Float64bits(v)
		buffer = append(buffer, make([]byte, 8)...)
		binary.LittleEndian.PutUint64(buffer, bits)
		return jsonTypeDouble, buffer, nil

	case []any:
		// MySQL attempts to use the small encoding first, and if offset sizes overflow, then it switches to the
		// large encoding. This is a little messy/inefficient to try the small encoding first, but because of the
		// way the binary format is designed, we can't know if/when we'll need the large format without serializing
		// the data first.
		id, encodedArray, err := encodeJsonArray(v, false)
		if err == nil {
			return id, encodedArray, nil
		}
		return encodeJsonArray(v, true)

	case map[string]any:
		// See the comment above about MySQL's JSON serialization format, and why we try the small encoding first,
		// before we know if we need the large encoding or not.
		id, encodedObject, err := encodeJsonObject(v, false)
		if err == nil {
			return id, encodedObject, nil
		}
		return encodeJsonObject(v, true)

	default:
		return 0x00, nil, fmt.Errorf("unexpected type in JSON document: %T", v)
	}
}

// appendForEncoding appends the |value| to the specified |bytes| and returns the updated byte slice. If
// |largeEncoding| is true, then 4 bytes are added to |bytes| to represent |value|, otherwise 2 bytes are used.
// This is a helper function for serializing the smallArray/largeArray and smallObject/largeObject formats, since
// they are identical formats, except that offsets, counts, and sizes are stored as 2 bytes in the small encodings,
// and stored as 4 bytes in the large encodings.
func appendForEncoding(bytes []byte, value uint32, largeEncoding bool) []byte {
	if !largeEncoding {
		bytes = append(bytes, byte(value), byte(value>>8))
	} else {
		bytes = append(bytes, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
	}
	return bytes
}

// appendStringLength appends a variable number of bytes to the specified |bytes| to encode |length|, the
// length of a string. For string lengths, if the length is larger than 127 bytes, we set the high bit of
// the first byte and use two bytes to encode the length. Similarly, if the high bit of the second byte is
// also set, the length is encoded over three bytes.
func appendStringLength(bytes []byte, length int) ([]byte, error) {
	switch {
	case length > 0x1FFFFF:
		return nil, fmt.Errorf("strings larger than 2,097,151 bytes not supported")

	case length > 0x3FFF: // 16,383
		return append(bytes,
			byte(length&0x7F|0x80),
			byte(length>>7|0x80),
			byte(length>>14)), nil

	case length > 0x7F: // 127
		return append(bytes,
			byte(length&0x7F|0x80),
			byte(length>>7)), nil

	default:
		return append(bytes, byte(length)), nil
	}
}

// calculateInitialArrayValuesOffset returns the initial offset value for the first array value in the
// encoded array byte slice. When |largeEncoding| is false, this value includes the two bytes for the
// element count, the two bytes for the encoded size, and three bytes (one byte for type ID, and two
// bytes for the offset) for each element in the array, specified by |arrayLength|. When |largeEncoding|
// is true, this value includes four bytes for the element count, four bytes for the encoded size, and
// five bytes (one byte for type ID, and four bytes for the offset) for each element in the array,
// specified by |arrayLength|.
func calculateInitialArrayValuesOffset(arrayLength int, largeEncoding bool) uint32 {
	if !largeEncoding {
		return uint32(2 + 2 + (arrayLength * 3))
	}
	return uint32(4 + 4 + (arrayLength * 5))
}

// calculateInitialObjectKeysOffset returns the initial offset value for the first key in the encoded
// object byte slice. When |largeEncoding| is false, the first key offset position is calculated by adding
// 2 bytes (the key/value pair count field), 2 bytes (the size of encoded object field), 4 bytes (2 bytes
// for the key offset, and 2 bytes for the length of the key) per key/value pair, specified by |objectLength|,
// and another 3 bytes (1 byte for the value's type ID, and 2 bytes for the offset to the value's data) for
// each key/value pair. When |largeEncoding| is true, the first key offset position is calculated by adding
// 4 bytes (the key/value pair count field), 4 bytes (the size of encoded object field), 6 bytes (4 bytes
// for the key offset, and 2 bytes for the length of the key) per key/value pair, specified by |objectLength|,
// and another 5 bytes (1 byte for the value's type ID, and 4 bytes for the offset to the value's data) for
// each key/value pair.
func calculateInitialObjectKeysOffset(objectLength int, largeEncoding bool) uint32 {
	if !largeEncoding {
		return uint32(2 + 2 + objectLength*4 + objectLength*3)
	}
	return uint32(4 + 4 + objectLength*6 + objectLength*5)
}
