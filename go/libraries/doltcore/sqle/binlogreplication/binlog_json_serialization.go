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
	"encoding/binary"
	"fmt"
	"math"

	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
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

// encodeJsonDoc encodes the specified |jsonDoc| into MySQL's custom/internal binary encoding
// so that it can be included in a binlog event.
//
// The internal MySQL JSON binary format is documented here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/json__binary_8h.html
//
// And a third-party description is here:
// https://lafengnan.gitbooks.io/blog/content/mysql/chapter2.html
func encodeJsonDoc(jsonDoc any) (buffer []byte, err error) {
	if jsonDoc == nil {
		buffer = append(buffer, jsonTypeLiteral)
		buffer = append(buffer, jsonLiteralValueNull)
		return buffer, nil
	}

	switch v := jsonDoc.(type) {
	case gmstypes.JSONDocument:
		return encodeJsonDoc(v.Val)

	case bool, string, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		typeId, encodedValue, err := encodeJsonValue(v)
		buffer = append(buffer, typeId)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, encodedValue...)

	case []any:
		typeId, encodedArray, err := encodeJsonArray(v)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, typeId)
		buffer = append(buffer, encodedArray...)

	case map[string]any:
		typeId, encodedObj, err := encodeJsonObject(v)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, typeId)
		buffer = append(buffer, encodedObj...)

	default:
		return nil, fmt.Errorf("unexpected type in JSON document: %T", v)
	}

	return buffer, nil
}

// encodeJsonArray encodes the specified |jsonArray| into MySQL's internal JSON encoding and returns
// the type ID indicating whether this is a small or large array, the encoded array data, and any
// error encountered.
//
// A JSON Array is encoded into the following components:
// - Type Identifier (1 byte): either jsonTypeSmallArray or jsonTypeLargeArray
// - Count (2 bytes for jsonTypeSmallArray, otherwise 4 bytes): the number of elements in the array
// - Size (2 bytes for jsonTypeSmallArray, otherwise 4 bytes): the total size of the encoded array (i.e. everything but the Type ID)
// - Value Entries (variable): 1 per value; 1 byte for type ID, 2 bytes for offset or inlined literal value for jsonTypeSmallArray, otherwise 4
// - Values (variable): 1 per value; encoded value bytes
func encodeJsonArray(jsonArray []any) (typeId byte, encodedArray []byte, err error) {
	var valueEntriesBuffer []byte
	var valuesBuffer []byte

	// nextValuesOffset starts at the byte offset in the encoded array where values start.
	// That includes the two bytes (for small arrays) for the element count, the two bytes
	// (for small arrays) for the encoded size, and three bytes for each element in the array.
	nextValuesOffset := uint16(2 + 2 + (len(jsonArray) * 3))

	for _, element := range jsonArray {
		typeId, buffer, err := encodeJsonValue(element)
		if err != nil {
			return 0, nil, err
		}

		// Literals can be inlined in the value-entries section
		if typeId == jsonTypeLiteral {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			if len(buffer) != 1 {
				return 0, nil, fmt.Errorf("unexpected buffer length")
			}
			valueEntriesBuffer = append(valueEntriesBuffer, buffer[0], byte(0))
		} else {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			valueEntriesBuffer = append(valueEntriesBuffer, byte(nextValuesOffset), byte(nextValuesOffset<<8))
			valuesBuffer = append(valuesBuffer, buffer...)
			nextValuesOffset += uint16(len(buffer))
		}
	}

	// element count (uint16 for small arrays)
	encodedArray = append(encodedArray, byte(len(jsonArray)), byte(len(jsonArray)<<8))
	// data payload size in bytes (uint16 for small arrays)
	// includes the two fields for element count and payload length (uint16s small arrays)
	arrayPayloadLength := 2 + 2 + len(valueEntriesBuffer) + len(valuesBuffer)
	encodedArray = append(encodedArray, byte(arrayPayloadLength), byte(arrayPayloadLength<<8))
	encodedArray = append(encodedArray, valueEntriesBuffer...)
	encodedArray = append(encodedArray, valuesBuffer...)

	return jsonTypeSmallArray, encodedArray, nil
}

// encodeJsonObject encodes the specified |jsonObject| into MySQL's internal JSON encoding and returns
// the type ID indicating whether this is a small or large object, the encoded object data, and any
// error encountered.
//
// A JSON Object is encoded into the following components:
// - Type Identifier (1 byte): either jsonTypeSmallObject or jsonTypeLargeObject
// - Count (2 bytes for jsonTypeSmallObject, otherwise 4 bytes): the number of keys in the object
// - Size (2 bytes for jsonTypeSmallObject, otherwise 4 bytes): the total size of the encoded object (i.e. everything but the Type ID)
// - Key Entries (variable): 1 per key; 2 bytes for key offset for jsonTypeSmallObject, otherwise 4; 2 bytes for key length
// - Value Entries (variable): 1 per value; 1 byte for type ID, 2 bytes for offset or inlined literal value for jsonTypeSmallObject, otherwise 4
// - Keys (variable): 1 per key; encoded string bytes
// - Values (variable): 1 per value; encoded value bytes
func encodeJsonObject(jsonObject map[string]any) (typeId byte, encodedObject []byte, err error) {
	var keyEntriesBuffer []byte
	var keysBuffer []byte
	// Since the key entries and value entries sections have a size based off of the number of key/value
	// pairs in the object, we can compute the first key offset position by adding 2 bytes (key/value
	// pair count field), 2 bytes (size of encoded object field), 4 bytes per key/value pair (one key entry
	// for each key/value pair), and 3 bytes for each key/value pair (one value entry for each key/value
	// pair).
	// NOTE: When we support large JSON objects, this will need to change to account for uint32 values
	//       instead of uint16 values.
	nextKeysOffset := uint16(2 + 2 + len(jsonObject)*4 + len(jsonObject)*3)

	// Process keys first, since value entry data depends on offsets that we don't know until we
	// process all the keys.
	for key, _ := range jsonObject {
		// NOTE: Don't use encodeJsonValue for the key – its length gets encoded slightly differently
		//       for JSON objects.
		encodedValue := []byte(key)

		keyEntriesBuffer = append(keyEntriesBuffer, byte(nextKeysOffset), byte(nextKeysOffset<<8))
		keyEntriesBuffer = append(keyEntriesBuffer, byte(len(encodedValue)), byte(len(encodedValue)<<8))
		keysBuffer = append(keysBuffer, encodedValue...)
		nextKeysOffset += uint16(len(encodedValue))
	}

	// Process values – since the object values are written after the keys, and we need to store the
	// offsets to those locations in the value entries that appear before the keys and the values, we
	// have to make a second pass through the object to process the values once we know the final
	// length of the keys section.
	var valueEntriesBuffer []byte
	var valuesBuffer []byte
	nextValuesOffset := nextKeysOffset
	for _, value := range jsonObject {
		typeId, buffer, err := encodeJsonValue(value)
		if err != nil {
			return 0, nil, err
		}

		// Literals may be inlined in the value-entries section
		if typeId == jsonTypeLiteral {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			if len(buffer) != 1 {
				return 0, nil, fmt.Errorf("unexpected buffer length")
			}
			valueEntriesBuffer = append(valueEntriesBuffer, buffer[0], byte(0))
		} else {
			valueEntriesBuffer = append(valueEntriesBuffer, typeId)
			valueEntriesBuffer = append(valueEntriesBuffer, byte(nextValuesOffset), byte(nextValuesOffset<<8))
			valuesBuffer = append(valuesBuffer, buffer...)
			nextValuesOffset += uint16(len(buffer))
		}
	}

	// element count (uint16 for small objects)
	encodedObject = append(encodedObject, byte(len(jsonObject)), byte(len(jsonObject)<<8))
	// data payload size in bytes (uint16 for small objects)
	// includes the two fields for element count and payload length (uint16s small arrays)
	arrayPayloadLength := 2 + 2 + len(keyEntriesBuffer) + len(keysBuffer) + len(valueEntriesBuffer) + len(valuesBuffer)
	encodedObject = append(encodedObject, byte(arrayPayloadLength), byte(arrayPayloadLength<<8))
	encodedObject = append(encodedObject, keyEntriesBuffer...)
	encodedObject = append(encodedObject, valueEntriesBuffer...)
	encodedObject = append(encodedObject, keysBuffer...)
	encodedObject = append(encodedObject, valuesBuffer...)

	return jsonTypeSmallObject, encodedObject, nil
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
		if len(v) > 127 {
			// TODO: data-length for string uses the high bit to indicate if additional
			//       bytes are needed for the data length field.
			return 0, nil, fmt.Errorf("strings larger than 127 bytes not supported yet!")
		}
		buffer = append(buffer, byte(len(v)))
		buffer = append(buffer, []byte(v)...)
		return jsonTypeString, buffer, nil

	case float64:
		// TODO: all our numbers end up being represented as float64s currently when we parse stored JSON
		bits := math.Float64bits(v)
		buffer = append(buffer, make([]byte, 8)...)
		binary.LittleEndian.PutUint64(buffer, bits)
		return jsonTypeDouble, buffer, nil

	case []any:
		return encodeJsonArray(v)

	case map[string]any:
		return encodeJsonObject(v)

	default:
		return 0x00, nil, fmt.Errorf("unexpected type in JSON document: %T", v)
	}
}
