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

package val

type Type uint8

func FixedSize(t Type) bool {
	return uint8(t) >= fixedSize
}

func SizeOf(t Type) byteSize {
	return sizeOfType[t]
}

type byteSize uint16

const (
	NullType      Type = 0
	Int8Type      Type = 1
	Uint8Type     Type = 2
	Int16Type     Type = 3
	Uint16Type    Type = 4
	Int24Type     Type = 5
	Uint24Type    Type = 6
	Int32Type     Type = 7
	Uint32Type    Type = 8
	Int64Type     Type = 9
	Uint64Type    Type = 10
	Float32Type   Type = 11
	Float64Type   Type = 12
	TimestampType Type = 13
	DateType      Type = 14
	TimeType      Type = 15
	DatetimeType  Type = 16
	YearType      Type = 17

	DecimalType Type = 128
	BitType     Type = 129

	CharType    Type = 130
	VarCharType Type = 131
	TextType    Type = 132

	BinaryType    Type = 133
	VarBinaryType Type = 134
	BlobType      Type = 135

	JSONType Type = 136

	EnumType Type = 137
	SetType  Type = 138

	ExpressionType Type = 139

	GeometryType Type = 140
)

const fixedSize uint8 = 128

var sizeOfType = [18]byteSize{
	1, // NullType
	1, // Int8Type
	1, // Uint8Type
	2, // Int16Type
	2, // Uint16Type
	3, // Int24Type
	3, // Uint24Type
	4, // Int32Type
	4, // Uint32Type
	8, // Int64Type
	8, // Uint64Type
	4, // Float32Type
	8, // Float64Type
	4, // TimestampType
	3, // DateType
	3, // TimeType
	8, // DatetimeType
	1, // YearType
}
