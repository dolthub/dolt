// Copyright 2020 Liquidata, Inc.
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

package typeinfo

import (
	"fmt"
	"strconv"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	bitTypeParam_Bits = "bits"
)

// This is a dolt implementation of the MySQL type Bit, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type bitImpl struct {
	sqlBitType sql.BitType
}

var _ TypeInfo = (*bitImpl)(nil)

func CreateBitTypeFromParams(params map[string]string) (TypeInfo, error) {
	if bitStr, ok := params[bitTypeParam_Bits]; ok {
		bitUint, err := strconv.ParseUint(bitStr, 10, 8)
		if err != nil {
			return nil, err
		}
		sqlBitType, err := sql.CreateBitType(uint8(bitUint))
		if err != nil {
			return nil, err
		}
		return &bitImpl{sqlBitType}, nil
	} else {
		return nil, fmt.Errorf(`create bit type info is missing param "%v"`, bitTypeParam_Bits)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *bitImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		res, err := ti.sqlBitType.Convert(uint64(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *bitImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	if artifact, ok := ti.isValid(v); ok {
		switch v.(type) {
		case nil, types.Null:
			return types.NullValue, nil
		}
		return types.Uint(artifact), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *bitImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*bitImpl); ok {
		return ti.sqlBitType.NumberOfBits() == ti2.sqlBitType.NumberOfBits()
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *bitImpl) GetTypeIdentifier() Identifier {
	return BitTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *bitImpl) GetTypeParams() map[string]string {
	return map[string]string{
		bitTypeParam_Bits: strconv.FormatInt(int64(ti.sqlBitType.NumberOfBits()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *bitImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *bitImpl) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *bitImpl) String() string {
	return fmt.Sprintf("Bit(%v)", ti.sqlBitType.NumberOfBits())
}

// ToSqlType implements TypeInfo interface.
func (ti *bitImpl) ToSqlType() sql.Type {
	return ti.sqlBitType
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *bitImpl) isValid(v interface{}) (artifact uint64, ok bool) {
	// convert some Noms values to their standard golang equivalents, except Null
	switch val := v.(type) {
	case nil:
		return 0, true
	case types.Null:
		return 0, true
	case types.Bool:
		v = bool(val)
	case types.Int:
		v = int64(val)
	case types.Uint:
		v = uint64(val)
	case types.Float:
		v = float64(val)
	case types.String:
		v = string(val)
	}
	res, err := ti.sqlBitType.Convert(v)
	resUint, ok := res.(uint64)
	return resUint, err == nil && ok
}
