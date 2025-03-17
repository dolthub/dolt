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

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/hash"
)

func init() {
	if v := os.Getenv(dconfig.EnvDisableFixedAccess); v != "" {
		disableFixedAccess = true
	}
}

// disableFixedAccess disables fast-access optimizations for
// not-null, fixed-width tuple values. See |makeFixedAccess|.
var disableFixedAccess = false

// TupleDesc describes a Tuple set.
// Data structures that contain Tuples and algorithms that process Tuples
// use a TupleDesc's types to interpret the fields of a Tuple.
type TupleDesc struct {
	Types    []Type
	Handlers []TupleTypeHandler
	cmp      TupleComparator
	fast     FixedAccess
}

// TupleTypeHandler is used to specifically handle types that use extended encoding. Such types are declared by GMS, and
// this is a forward reference for the interface functions that are necessary here.
type TupleTypeHandler interface {
	// SerializedCompare compares two byte slices that each represent a serialized value, without first deserializing
	// the value.
	SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error)
	// SerializeValue converts the given value into a binary representation.
	SerializeValue(ctx context.Context, val any) ([]byte, error)
	// DeserializeValue converts a binary representation of a value into its canonical type.
	DeserializeValue(ctx context.Context, val []byte) (any, error)
	// FormatValue returns a string version of the value. Primarily intended for display.
	FormatValue(val any) (string, error)
}

// TupleDescriptorArgs are a set of optional arguments for TupleDesc creation.
type TupleDescriptorArgs struct {
	Comparator TupleComparator
	Handlers   []TupleTypeHandler
}

// NewTupleDescriptor makes a TupleDescriptor from |types|.
func NewTupleDescriptor(types ...Type) TupleDesc {
	return NewTupleDescriptorWithArgs(TupleDescriptorArgs{}, types...)
}

// NewTupleDescriptorWithArgs returns a TupleDesc based on the given arguments.
func NewTupleDescriptorWithArgs(args TupleDescriptorArgs, types ...Type) (td TupleDesc) {
	if len(types) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}
	for _, typ := range types {
		if typ.Enc == NullEnc {
			panic("invalid encoding")
		}
	}
	if args.Comparator == nil {
		args.Comparator = DefaultTupleComparator{}
	}
	args.Comparator = ExtendedTupleComparator{args.Comparator, args.Handlers}.Validated(types)

	td = TupleDesc{
		Types:    types,
		Handlers: args.Handlers,
		cmp:      args.Comparator,
		fast:     makeFixedAccess(types),
	}
	return
}

func IterAddressFields(td TupleDesc, cb func(int, Type)) {
	for i, typ := range td.Types {
		switch typ.Enc {
		case BytesAddrEnc, StringAddrEnc,
			JSONAddrEnc, CommitAddrEnc, GeomAddrEnc:
			cb(i, typ)
		}
	}
}

func IterToastFields(td TupleDesc, cb func(int, Type)) {
	for i, typ := range td.Types {
		switch typ.Enc {
		case BytesToastEnc, StringToastEnc:
			cb(i, typ)
		}
	}
}

type FixedAccess [][2]ByteSize

func makeFixedAccess(types []Type) (acc FixedAccess) {
	if disableFixedAccess {
		return nil
	}

	acc = make(FixedAccess, 0, len(types))

	off := ByteSize(0)
	for _, typ := range types {
		if typ.Nullable {
			break
		}
		sz, ok := sizeFromType(typ)
		if !ok {
			break
		}
		acc = append(acc, [2]ByteSize{off, off + sz})
		off += sz
	}
	return
}

func (td TupleDesc) AddressFieldCount() (n int) {
	IterAddressFields(td, func(int, Type) {
		n++
	})
	return
}

// PrefixDesc returns a descriptor for the first n types.
func (td TupleDesc) PrefixDesc(n int) TupleDesc {
	if len(td.Handlers) == 0 {
		return NewTupleDescriptorWithArgs(TupleDescriptorArgs{Comparator: td.cmp.Prefix(n)}, td.Types[:n]...)
	}
	return NewTupleDescriptorWithArgs(TupleDescriptorArgs{Comparator: td.cmp.Prefix(n), Handlers: td.Handlers[:n]}, td.Types[:n]...)
}

// GetField returns the ith field of |tup|.
func (td TupleDesc) GetField(i int, tup Tuple) []byte {
	if i < len(td.fast) {
		start, stop := td.fast[i][0], td.fast[i][1]
		return tup[start:stop]
	}
	return tup.GetField(i)
}

// Compare compares |left| and |right|.
func (td TupleDesc) Compare(ctx context.Context, left, right Tuple) (cmp int) {
	return td.cmp.Compare(ctx, left, right, td)
}

// CompareField compares |value| with the ith field of |tup|.
func (td TupleDesc) CompareField(ctx context.Context, value []byte, i int, tup Tuple) (cmp int) {
	var v []byte
	if i < len(td.fast) {
		start, stop := td.fast[i][0], td.fast[i][1]
		v = tup[start:stop]
	} else {
		v = tup.GetField(i)
	}
	return td.cmp.CompareValues(ctx, i, value, v, td.Types[i])
}

// Comparator returns the TupleDescriptor's TupleComparator.
func (td TupleDesc) Comparator() TupleComparator {
	return td.cmp
}

// Count returns the number of fields in the TupleDesc.
func (td TupleDesc) Count() int {
	return len(td.Types)
}

// IsNull returns true if the ith field of the Tuple is NULL.
func (td TupleDesc) IsNull(i int, tup Tuple) bool {
	b := td.GetField(i, tup)
	return b == nil
}

func (td TupleDesc) HasNulls(tup Tuple) bool {
	if tup.Count() < td.Count() {
		return true
	}
	for i := range td.Types {
		if tup.FieldIsNull(i) {
			return true
		}
	}
	return false
}

// GetFixedAccess returns the FixedAccess for this tuple descriptor.
func (td TupleDesc) GetFixedAccess() FixedAccess {
	return td.fast
}

// WithoutFixedAccess returns a copy of |td| without fixed access metadata.
func (td TupleDesc) WithoutFixedAccess() TupleDesc {
	return TupleDesc{Types: td.Types, Handlers: td.Handlers, cmp: td.cmp}
}

// GetBool reads a bool from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBool(i int, tup Tuple) (v bool, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readBool(b), true
	}
	return
}

// GetInt8 reads an int8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt8(i int, tup Tuple) (v int8, ok bool) {
	td.expectEncoding(i, Int8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt8(b), true
	}
	return
}

// GetUint8 reads a uint8 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint8(i int, tup Tuple) (v uint8, ok bool) {
	td.expectEncoding(i, Uint8Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint8(b), true
	}
	return
}

// GetInt16 reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt16(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, Int16Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt16(b), true
	}
	return
}

// GetUint16 reads a uint16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint16(i int, tup Tuple) (v uint16, ok bool) {
	td.expectEncoding(i, Uint16Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = ReadUint16(b), true
	}
	return
}

// GetInt32 reads an int32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt32(i int, tup Tuple) (v int32, ok bool) {
	td.expectEncoding(i, Int32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt32(b), true
	}
	return
}

// GetUint32 reads a uint32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint32(i int, tup Tuple) (v uint32, ok bool) {
	td.expectEncoding(i, Uint32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = ReadUint32(b), true
	}
	return
}

// GetInt64 reads an int64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetInt64(i int, tup Tuple) (v int64, ok bool) {
	td.expectEncoding(i, Int64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt64(b), true
	}
	return
}

// GetUint64 reads a uint64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetUint64(i int, tup Tuple) (v uint64, ok bool) {
	td.expectEncoding(i, Uint64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readUint64(b), true
	}
	return
}

// GetFloat32 reads a float32 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat32(i int, tup Tuple) (v float32, ok bool) {
	td.expectEncoding(i, Float32Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readFloat32(b), true
	}
	return
}

// GetFloat64 reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetFloat64(i int, tup Tuple) (v float64, ok bool) {
	td.expectEncoding(i, Float64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readFloat64(b), true
	}
	return
}

// GetBit reads a uint64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBit(i int, tup Tuple) (v uint64, ok bool) {
	td.expectEncoding(i, Bit64Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readBit64(b), true
	}
	return
}

// GetDecimal reads a float64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetDecimal(i int, tup Tuple) (v decimal.Decimal, ok bool) {
	td.expectEncoding(i, DecimalEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readDecimal(b), true
	}
	return
}

// GetYear reads an int16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetYear(i int, tup Tuple) (v int16, ok bool) {
	td.expectEncoding(i, YearEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readYear(b), true
	}
	return
}

// GetDate reads a time.Time from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetDate(i int, tup Tuple) (v time.Time, ok bool) {
	td.expectEncoding(i, DateEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readDate(b), true
	}
	return
}

// GetSqlTime reads an int64 encoded Time value, representing a duration as a number of microseconds,
// from the ith field of the Tuple. If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetSqlTime(i int, tup Tuple) (v int64, ok bool) {
	td.expectEncoding(i, TimeEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readInt64(b), true
	}
	return
}

// GetDatetime reads a time.Time from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetDatetime(i int, tup Tuple) (v time.Time, ok bool) {
	td.expectEncoding(i, DatetimeEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readDatetime(b), true
	}
	return
}

// GetEnum reads a uin16 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetEnum(i int, tup Tuple) (v uint16, ok bool) {
	td.expectEncoding(i, EnumEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readEnum(b), true
	}
	return
}

// GetSet reads a uint64 from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetSet(i int, tup Tuple) (v uint64, ok bool) {
	td.expectEncoding(i, SetEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v, ok = readSet(b), true
	}
	return
}

// GetString reads a string from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetString(i int, tup Tuple) (v string, ok bool) {
	td.expectEncoding(i, StringEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readString(b)
		ok = true
	}
	return
}

// GetBytes reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetBytes(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, ByteStringEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
		ok = true
	}
	return
}

// GetJSON reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetJSON(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, JSONEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
		ok = true
	}
	return
}

// GetGeometry reads a []byte from the ith field of the Tuple.
// If the ith field is NULL, |ok| is set to false.
func (td TupleDesc) GetGeometry(i int, tup Tuple) (v []byte, ok bool) {
	// TODO: we are support both Geometry and GeometryAddr for now, so we can't expect just one
	// td.expectEncoding(i, GeometryEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readByteString(b)
		ok = true
	}
	return
}

func (td TupleDesc) GetGeometryAddr(i int, tup Tuple) (hash.Hash, bool) {
	// TODO: we are support both Geometry and GeometryAddr for now, so we can't expect just one
	// td.expectEncoding(i, GeomAddrEnc)
	return td.getAddr(i, tup)
}

func (td TupleDesc) GetHash128(i int, tup Tuple) (v []byte, ok bool) {
	td.expectEncoding(i, Hash128Enc)
	b := td.GetField(i, tup)
	if b != nil {
		v = b
		ok = true
	}
	return
}

// GetExtended reads a byte slice from the ith field of the Tuple.
func (td TupleDesc) GetExtended(i int, tup Tuple) ([]byte, bool) {
	td.expectEncoding(i, ExtendedEnc)
	v := td.GetField(i, tup)
	return v, v != nil
}

// GetExtendedAddr reads a hash from the ith field of the Tuple.
func (td TupleDesc) GetExtendedAddr(i int, tup Tuple) (hash.Hash, bool) {
	td.expectEncoding(i, ExtendedAddrEnc)
	return td.getAddr(i, tup)
}

func (td TupleDesc) GetJSONAddr(i int, tup Tuple) (hash.Hash, bool) {
	td.expectEncoding(i, JSONAddrEnc)
	return td.getAddr(i, tup)
}

func (td TupleDesc) GetStringAddr(i int, tup Tuple) (hash.Hash, bool) {
	td.expectEncoding(i, StringAddrEnc)
	return td.getAddr(i, tup)
}

func (td TupleDesc) GetBytesAddr(i int, tup Tuple) (hash.Hash, bool) {
	td.expectEncoding(i, BytesAddrEnc)
	return td.getAddr(i, tup)
}

// GetBytesToastValue returns either a []byte or a BytesWrapper, but Go doesn't allow us to use a single type for that.
func (td TupleDesc) GetBytesToastValue(i int, vs ValueStore, tup Tuple) (interface{}, bool, error) {
	td.expectEncoding(i, BytesToastEnc)
	toastValue := ToastValue(td.GetField(i, tup))
	if len(toastValue) == 0 {
		return nil, false, nil
	}
	if toastValue.isInlined() {
		// TODO: This needs context.
		val, err := toastValue.convertToBytes(nil, vs, nil)
		return val, true, err
	} else {
		val, err := toastValue.convertToByteArray(nil, vs, nil)
		return val, true, err
	}
}

func (td TupleDesc) GetStringToastValue(i int, vs ValueStore, tup Tuple) (interface{}, bool, error) {
	td.expectEncoding(i, StringToastEnc)
	toastValue := ToastValue(td.GetField(i, tup))
	if len(toastValue) == 0 {
		return nil, false, nil
	}
	if toastValue.isInlined() {
		// TODO: This needs context.
		val, err := toastValue.convertToBytes(nil, vs, nil)
		return string(val), true, err
	} else {
		val, err := toastValue.convertToTextStorage(nil, vs, nil)
		return val, true, err
	}
}

func (td TupleDesc) GetCommitAddr(i int, tup Tuple) (v hash.Hash, ok bool) {
	td.expectEncoding(i, CommitAddrEnc)
	return td.getAddr(i, tup)
}

func (td TupleDesc) getAddr(i int, tup Tuple) (hash.Hash, bool) {
	b := td.GetField(i, tup)
	if b == nil {
		return hash.Hash{}, false
	}
	return hash.New(b), true
}

func (td TupleDesc) expectEncoding(i int, encodings ...Encoding) {
	for _, enc := range encodings {
		if enc == td.Types[i].Enc {
			return
		}
	}
	panic("incorrect value encoding")
}

func (td TupleDesc) GetCell(i int, tup Tuple) (v Cell, ok bool) {
	td.expectEncoding(i, CellEnc)
	b := td.GetField(i, tup)
	if b != nil {
		v = readCell(b)
		ok = true
	}
	return
}

// Format prints a Tuple as a string.
func (td TupleDesc) Format(ctx context.Context, tup Tuple) string {
	if tup == nil || tup.Count() == 0 {
		return "( )"
	}

	var sb strings.Builder
	sb.WriteString("( ")

	seenOne := false
	for i := range td.Types {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true
		sb.WriteString(td.FormatValue(ctx, i, tup.GetField(i)))
	}
	sb.WriteString(" )")
	return sb.String()
}

func (td TupleDesc) FormatValue(ctx context.Context, i int, value []byte) string {
	if value == nil {
		return "NULL"
	}
	return td.formatValue(ctx, td.Types[i].Enc, i, value)
}

func (td TupleDesc) formatValue(ctx context.Context, enc Encoding, i int, value []byte) string {
	switch enc {
	case Int8Enc:
		v := readInt8(value)
		return strconv.Itoa(int(v))
	case Uint8Enc:
		v := readUint8(value)
		return strconv.Itoa(int(v))
	case Int16Enc:
		v := readInt16(value)
		return strconv.Itoa(int(v))
	case Uint16Enc:
		v := ReadUint16(value)
		return strconv.Itoa(int(v))
	case Int32Enc:
		v := readInt32(value)
		return strconv.Itoa(int(v))
	case Uint32Enc:
		v := ReadUint32(value)
		return strconv.Itoa(int(v))
	case Int64Enc:
		v := readInt64(value)
		return strconv.FormatInt(v, 10)
	case Uint64Enc:
		v := readUint64(value)
		return strconv.FormatUint(v, 10)
	case Float32Enc:
		v := readFloat32(value)
		return fmt.Sprintf("%f", v)
	case Float64Enc:
		v := readFloat64(value)
		return fmt.Sprintf("%f", v)
	case Bit64Enc:
		v := readUint64(value)
		return strconv.FormatUint(v, 10)
	case DecimalEnc:
		v := readDecimal(value)
		return v.String()
	case YearEnc:
		v := readYear(value)
		return strconv.Itoa(int(v))
	case DateEnc:
		v := readDate(value)
		return v.Format("2006-01-02")
	case TimeEnc:
		v := readTime(value)
		return strconv.FormatInt(v, 10)
	case DatetimeEnc:
		v := readDatetime(value)
		return v.Format(time.RFC3339)
	case EnumEnc:
		v := readEnum(value)
		return strconv.Itoa(int(v))
	case SetEnc:
		v := readSet(value)
		return strconv.FormatUint(v, 10)
	case StringEnc:
		return readString(value)
	case ByteStringEnc:
		return hex.EncodeToString(value)
	case Hash128Enc:
		return hex.EncodeToString(value)
	case BytesAddrEnc:
		return hex.EncodeToString(value)
	case StringAddrEnc:
		return hex.EncodeToString(value)
	case CommitAddrEnc:
		return hex.EncodeToString(value)
	case CellEnc:
		return hex.EncodeToString(value)
	case ExtendedEnc:
		handler := td.Handlers[i]
		v := readExtended(ctx, handler, value)
		str, err := handler.FormatValue(v)
		if err != nil {
			panic(err)
		}
		return str
	case ExtendedAddrEnc:
		return hex.EncodeToString(value)
	default:
		return string(value)
	}
}

// Equals returns true if |td| and |other| have equal type slices.
func (td TupleDesc) Equals(other TupleDesc) bool {
	if len(td.Types) != len(other.Types) {
		return false
	}
	for i, typ := range td.Types {
		if typ != other.Types[i] {
			return false
		}
	}
	return true
}

// AddressTypeHandler is an implementation of TupleTypeHandler for columns that contain a content-address to some value
// stored in a ValueStore. This TypeHandler converts between the address and the underlying value as needed, allowing
// these columns to be used in contexts that need access to the underlying value, such as in primary indexes.
type AddressTypeHandler struct {
	vs           ValueStore
	childHandler TupleTypeHandler
}

func NewExtendedAddressTypeHandler(vs ValueStore, childHandler TupleTypeHandler) AddressTypeHandler {
	return AddressTypeHandler{
		vs:           vs,
		childHandler: childHandler,
	}
}

func (handler AddressTypeHandler) SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error) {
	// TODO: If the child handler allows, compare the values one chunk at a time instead of always fully deserializing them.
	var err error
	var v1Bytes []byte
	if len(v1) > 0 {
		v1Bytes, err = handler.vs.ReadBytes(ctx, hash.New(v1))
		if err != nil {
			return 0, err
		}
	}
	var v2Bytes []byte
	if len(v2) > 0 {
		v2Bytes, err = handler.vs.ReadBytes(ctx, hash.New(v2))
		if err != nil {
			return 0, err
		}
	}
	return handler.childHandler.SerializedCompare(ctx, v1Bytes, v2Bytes)
}

func (handler AddressTypeHandler) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	b, err := handler.childHandler.SerializeValue(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	h, err := handler.vs.WriteBytes(context.Background(), b)
	if err != nil {
		return nil, err
	}
	return h[:], err
}

func (handler AddressTypeHandler) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	if len(val) == 0 {
		return nil, nil
	}
	b, err := handler.vs.ReadBytes(ctx, hash.New(val))
	if err != nil {
		return nil, err
	}
	return handler.childHandler.DeserializeValue(ctx, b)
}

func (handler AddressTypeHandler) FormatValue(val any) (string, error) {
	return handler.childHandler.FormatValue(val)
}

type LengthAndTreeValue struct {
	length int
	ImmutableValue
}

var _ TupleTypeHandler = AddressTypeHandler{}

func getOversizedHeaderLength(firstByte byte) int {
	// for now always use 4 bytes for length
	return 9
}

type ToastValue []byte

func (v ToastValue) getOversizedOutlinedLength() int64 {
	return readInt64(v[1:9])
}

func headerLengthForBlobSize(blobSize int) int64 {
	return 8
}

func (v ToastValue) getOversizedBytes() []byte {
	if len(v) == 0 {
		return nil
	}
	if v[0]&128 == 1 {
		headerLength := getOversizedHeaderLength(v[0])
		return v[headerLength:]
	}
	return v[1:]
}

/*

// OversizedTypeHandler is an implementation of TupleTypeHandler for columns that embed short values, but
// for long values contain a content-address to some value stored in a ValueStore.
// This TypeHandler converts between the address and the underlying value as needed, allowing
// these columns to be used in contexts that need access to the underlying value, such as in primary indexes.
// Every value stored in this column has a header that describes how to interpret the rest of the value.
// We embed the size because this helps us determine whether we need to deserialize when writing a row.
// bit 0 - is the value stored in band or out of band
// bit 1 - is the value compressed. Currently not supported, always set to 0.
// reserve additional bits?
// size - variable length encoding?
type OversizedTypeHandler struct {
	vs           ValueStore
	childHandler TupleTypeHandler
}

func NewOversizedTypeHandler(vs ValueStore, childHandler TupleTypeHandler) OversizedTypeHandler {
	return OversizedTypeHandler{
		vs:           vs,
		childHandler: childHandler,
	}
}

func (handler OversizedTypeHandler) SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error) {
	// TODO: If the child handler allows, compare the values one chunk at a time instead of always fully deserializing them.
	var err error
	v1InlineBytes := getOversizedBytes(v1)
	var v1Bytes []byte
	if len(v1) > 0 {
		v1Bytes, err = handler.vs.ReadBytes(ctx, hash.New(v1InlineBytes))
		if err != nil {
			return 0, err
		}
	}
	v2InlineBytes := getOversizedBytes(v2)
	var v2Bytes []byte
	if len(v2) > 0 {
		v2Bytes, err = handler.vs.ReadBytes(ctx, hash.New(v2InlineBytes))
		if err != nil {
			return 0, err
		}
	}
	return handler.childHandler.SerializedCompare(ctx, v1Bytes, v2Bytes)
}

// we need to know the total length of the row, or handle this at some higher level.
func (handler OversizedTypeHandler) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	b, err := handler.childHandler.SerializeValue(ctx, val)
	if err != nil {
		return nil, err
	}
	h, err := handler.vs.WriteBytes(context.Background(), b)
	if err != nil {
		return nil, err
	}
	return h[:], err
}

func (handler OversizedTypeHandler) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	firstByte := val[0]
	if firstByte&128 == 0 {
		return val[1:], nil
	}
	// value is stored out of band
	h := getOversizedBytes(val)
	// properly convert this to an int
	size := readInt64(val[1:9])
	// TODO: Allow for returning BytesArray, or combine them.
	return NewTextStorage(hash.New(h), handler.vs).WithMaxByteLength(size), nil
}

func (handler OversizedTypeHandler) FormatValue(val any) (string, error) {
	return handler.childHandler.FormatValue(val)
}

var _ TupleTypeHandler = OversizedTypeHandler{}

*/

func (v ToastValue) outlineSize() int64 {
	if v.IsOutlined() {
		return int64(len(v))
	}
	blobLength := len(v) - 1                            // Get length without header byte
	return 1 + headerLengthForBlobSize(blobLength) + 20 // Header byte + variable length + address
}

func (v ToastValue) inlineSize() int64 {
	if v.isInlined() {
		return int64(len(v))
	}
	// read length of the outlined message and use it to compute the length if this were inlined.
	blobLength := v.getOversizedOutlinedLength()
	return 1 + blobLength
}

func (v ToastValue) IsNull() bool {
	return len(v) == 0
}

func (v ToastValue) IsOutlined() bool {
	if v.IsNull() {
		return false
	}
	return v[0]&128 != 0
}

// If a conversion is necessary, the converted value will be written into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v ToastValue) convertToOutline(ctx context.Context, vs ValueStore, buf []byte) (ToastValue, error) {
	if v.IsOutlined() {
		return v, nil
	}
	blob := v[1 : len(v)-1]
	blobLength := len(blob)
	blobHash, err := vs.WriteBytes(ctx, blob)
	if err != nil {
		return nil, err
	}
	dest := buf[len(buf):]

	outputSize := 1 + 8 + 20
	if cap(dest) < outputSize {
		dest = make([]byte, outputSize)
	}
	dest[0] = 128
	writeInt64(dest[1:9], int64(blobLength))
	copy(dest[9:29], blobHash[:])
	return dest, nil
}

func (v ToastValue) isInlined() bool {
	if v.IsNull() {
		return false
	}
	return v[0]&128 == 0
}

// If a conversion is necessary, the converted value will be written into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v ToastValue) convertToInline(ctx context.Context, vs ValueStore, buf []byte) (ToastValue, error) {
	if v.isInlined() {
		return v, nil
	}
	// blobLength := readInt64(bytes[1:9])
	addr := v[9:29]
	blob, err := vs.ReadBytes(ctx, hash.New(addr))
	if err != nil {
		return nil, err
	}
	dest := buf[len(buf):]
	outputSize := 1 + len(blob)
	if cap(dest) < outputSize {
		dest = make([]byte, outputSize)
	}
	dest[0] = 0
	dest = append(dest, blob...)
	return dest, nil
}

func (v ToastValue) convertToBytes(ctx context.Context, vs ValueStore, buf []byte) ([]byte, error) {
	// Only inlined values can be converted to bytes
	inlineValue, err := v.convertToInline(ctx, vs, buf)
	if err != nil {
		return nil, err
	}
	// Remove header byte and null terminator.
	return inlineValue[1 : len(inlineValue)-1], nil
}

func (v ToastValue) convertToByteArray(ctx context.Context, vs ValueStore, buf []byte) (*ByteArray, error) {
	// Only outlined values can be converted to a ByteArray
	outlineValue, err := v.convertToOutline(ctx, vs, buf)
	if err != nil {
		return &ByteArray{}, err
	}
	address := hash.New(outlineValue[9:29])
	length := readInt64(outlineValue[1:9])
	return NewByteArray(address, vs).WithMaxByteLength(length), nil
}

func (v ToastValue) convertToTextStorage(ctx context.Context, vs ValueStore, buf []byte) (*TextStorage, error) {
	// Only outlined values can be converted to a ByteArray
	outlineValue, err := v.convertToOutline(ctx, vs, buf)
	if err != nil {
		return &TextStorage{}, err
	}
	address := hash.New(outlineValue[9:29])
	length := readInt64(outlineValue[1:9])
	return NewTextStorage(address, vs).WithMaxByteLength(length), nil
}
