// Copyright 2020 Dolthub, Inc.
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
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	bitTypeParam_Bits = "bits"
)

// This is a dolt implementation of the MySQL type Bit, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type bitType struct {
	sqlBitType gmstypes.BitType
}

var _ TypeInfo = (*bitType)(nil)

var PseudoBoolType TypeInfo = &bitType{gmstypes.MustCreateBitType(1)}

func CreateBitTypeFromParams(params map[string]string) (TypeInfo, error) {
	if bitStr, ok := params[bitTypeParam_Bits]; ok {
		bitUint, err := strconv.ParseUint(bitStr, 10, 8)
		if err != nil {
			return nil, err
		}
		sqlBitType, err := gmstypes.CreateBitType(uint8(bitUint))
		if err != nil {
			return nil, err
		}
		return &bitType{sqlBitType}, nil
	} else {
		return nil, fmt.Errorf(`create bit type info is missing param "%v"`, bitTypeParam_Bits)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *bitType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		return uint64(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *bitType) ReadFrom(_ *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.UintKind:
		val := reader.ReadUint()
		return val, nil
	case types.NullKind:
		return nil, nil
	}

	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *bitType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	uintVal, _, err := ti.sqlBitType.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	val, ok := uintVal.(uint64)
	if ok {
		return types.Uint(val), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *bitType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*bitType); ok {
		return ti.sqlBitType.NumberOfBits() == ti2.sqlBitType.NumberOfBits()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *bitType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	uintVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := uintVal.(uint64)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	res := strconv.FormatUint(val, 10)
	return &res, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *bitType) GetTypeIdentifier() Identifier {
	return BitTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *bitType) GetTypeParams() map[string]string {
	return map[string]string{
		bitTypeParam_Bits: strconv.FormatInt(int64(ti.sqlBitType.NumberOfBits()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *bitType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Uint); ok {
		_, _, err := ti.sqlBitType.Convert(context.Background(), uint64(val))
		if err != nil {
			return false
		}
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *bitType) NomsKind() types.NomsKind {
	return types.UintKind
}

// Promote implements TypeInfo interface.
func (ti *bitType) Promote() TypeInfo {
	return &bitType{ti.sqlBitType.Promote().(gmstypes.BitType)}
}

// String implements TypeInfo interface.
func (ti *bitType) String() string {
	return fmt.Sprintf("Bit(%v)", ti.sqlBitType.NumberOfBits())
}

// ToSqlType implements TypeInfo interface.
func (ti *bitType) ToSqlType() sql.Type {
	return ti.sqlBitType
}

// bitTypeConverter is an internal function for GetTypeConverter that handles the specific type as the source TypeInfo.
func bitTypeConverter(ctx context.Context, src *bitType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	switch dest := destTi.(type) {
	case *bitType:
		if src.sqlBitType.NumberOfBits() <= dest.sqlBitType.NumberOfBits() {
			return identityTypeConverter, false, nil
		} else {
			return wrapIsValid(dest.IsValid, src, dest)
		}
	case *blobStringType:
		return bitTypeConverterInterpretAsString(ctx, src, destTi)
	case *boolType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *datetimeType:
		return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
			if v == nil || v == types.NullValue {
				return types.NullValue, nil
			}
			val, ok := v.(types.Uint)
			if !ok {
				return nil, fmt.Errorf("unexpected type converting bit to %s: %T", strings.ToLower(dest.String()), v)
			}
			if val == 0 {
				return types.Timestamp(gmstypes.Datetime.Zero().(time.Time)), nil
			}
			return nil, fmt.Errorf("invalid %s value: %d", strings.ToLower(dest.String()), uint64(val))
		}, true, nil
	case *decimalType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *enumType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *floatType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geomcollType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *geometryType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *inlineBlobType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *intType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *jsonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *linestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multilinestringType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *multipolygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *pointType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *polygonType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *setType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *timeType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	case *uintType:
		return wrapIsValid(dest.IsValid, src, dest)
	case *uuidType:
		return nil, false, IncompatibleTypeConversion.New(src.String(), destTi.String())
	case *varBinaryType:
		return bitTypeConverterInterpretAsString(ctx, src, destTi)
	case *varStringType:
		if dest.sqlStringType.Type() == sqltypes.Text {
			return bitTypeConverterInterpretAsString(ctx, src, destTi)
		} else {
			return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
		}
	case *yearType:
		return wrapConvertValueToNomsValue(dest.ConvertValueToNomsValue)
	default:
		return nil, false, UnhandledTypeConversion.New(src.String(), destTi.String())
	}
}

func bitTypeConverterInterpretAsString(ctx context.Context, src *bitType, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
		if v == nil || v == types.NullValue {
			return types.NullValue, nil
		}
		val, ok := v.(types.Uint)
		if !ok {
			return nil, fmt.Errorf("unexpected type interpreting bit as string: %T", v)
		}
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(val))

		numOfBits := src.sqlBitType.NumberOfBits()
		switch true {
		case numOfBits <= 8:
			bytes = bytes[:1]
		case numOfBits <= 16:
			bytes = bytes[:2]
		case numOfBits <= 24:
			bytes = bytes[:3]
		case numOfBits <= 32:
			bytes = bytes[:4]
		case numOfBits <= 40:
			bytes = bytes[:5]
		case numOfBits <= 48:
			bytes = bytes[:6]
		case numOfBits <= 56:
			bytes = bytes[:7]
		}
		// MySQL's BIT strings are reversed
		for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
			bytes[i], bytes[j] = bytes[j], bytes[i]
		}
		s := string(bytes)
		if dest, ok := destTi.(*varStringType); ok && dest.sqlStringType.Type() == sqltypes.Text {
			if !utf8.ValidString(s) {
				return nil, fmt.Errorf(`invalid %s value: "%s"`, strings.ToLower(dest.String()), s)
			}
		}
		return destTi.ConvertValueToNomsValue(ctx, vrw, s)
	}, true, nil
}
