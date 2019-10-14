// Copyright 2019 Liquidata, Inc.
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

package dolttypes

import (
	"fmt"
)

// DoltKind is Dolt's version of NomsKind for Dolt-defined types built on UnderlyingArrayKind.
type DoltKind byte
const DoltKindLength = 1

const (
	UnknownKind DoltKind = iota
	TimestampKind
)

type DoltType interface {
	// Compare returns whether the DoltType is equal to another DoltType. If different types, compares their kinds.
	Compare(DoltType) int
	// Decode takes in a byte array (prepended with the kind) and returns a new DoltType representing the given data.
	Decode([]byte) (DoltType, error)
	// Encode returns a byte array fully representing the type (prepended with the kind) suitable for decoding or storage.
	Encode() ([]byte, error)
	// Equals returns whether this DoltType represents the exact same data as another DoltType.
	Equals(DoltType) bool
	// Kind returns the DoltKind of this DoltType.
	Kind() DoltKind
	// MarshalBool returns a bool representing the DoltType.
	MarshalBool() (bool, error)
	// MarshalDoltType returns a DoltType (defined by the DoltKind) representing the parent DoltType.
	MarshalDoltType(DoltKind) (DoltType, error)
	// MarshalFloat returns a float64 representing the DoltType.
	MarshalFloat() (float64, error)
	// MarshalInt returns a int64 representing the DoltType.
	MarshalInt() (int64, error)
	// MarshalString returns a string representing the DoltType.
	MarshalString() (string, error)
	// MarshalUint returns a uint64 representing the DoltType.
	MarshalUint() (uint64, error)
	// UnmarshalBool returns this DoltType representing the given bool.
	UnmarshalBool(bool) (DoltType, error)
	// UnmarshalDoltType returns this DoltType representing the given DoltType, which may be of a different DoltKind.
	UnmarshalDoltType(DoltType) (DoltType, error)
	// UnmarshalFloat returns this DoltType representing the given float64.
	UnmarshalFloat(float64) (DoltType, error)
	// UnmarshalInt returns this DoltType representing the given int64.
	UnmarshalInt(int64) (DoltType, error)
	// UnmarshalString returns this DoltType representing the given string.
	UnmarshalString(string) (DoltType, error)
	// UnmarshalUint returns this DoltType representing the given uint64.
	UnmarshalUint(uint64) (DoltType, error)
	fmt.Stringer
}

var kindMap = map[DoltKind]DoltType {
	UnknownKind:   Unknown{},
	TimestampKind: Timestamp{},
}
var KindName = map[DoltKind]string {
	UnknownKind:   "unknown",
	TimestampKind: "Timestamp",
}

// Creates a DoltType given an array of bytes representative of a full DoltType.
// Will always return either the contained type or Unknown.
func DecodeDoltType(data []byte) (DoltType, error) {
	if len(data) > 0 {
		dt := DoltKind(data[0]).Type()
		return dt.Decode(data)
	}
	return Unknown{}, fmt.Errorf("empty byte array cannot create DoltType")
}

// MustEncode panics if encoding fails. Useful for tests.
func MustEncode(doltType DoltType) []byte {
	data, err := doltType.Encode()
	if err != nil {
		panic(err)
	}
	return data
}

// Compare returns whether the kind is equal to or greater/less than another kind.
func (k DoltKind) Compare(other DoltKind) int {
	if k < other {
		return -1
	} else if k == other {
		return 0
	}
	return 1
}

// PrependKind adds the kind to the beginning of a byte array. Helper method.
func (k DoltKind) PrependKind(data []byte) []byte {
	return append([]byte{byte(k)}, data...)
}

// String implements fmt.Stringer
func (k DoltKind) String() string {
	return KindName[k]
}

// Type returns the DoltType associated with this kind, or Unknown if the kind does not exist.
// The DoltType can be used for decoding and unmarshalling.
func (k DoltKind) Type() DoltType {
	if dt, ok := kindMap[k]; ok {
		return dt
	}
	return Unknown{}
}

