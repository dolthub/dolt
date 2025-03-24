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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mohae/uvarint"
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

func IterAdaptiveFields(td TupleDesc, cb func(int, Type)) {
	for i, typ := range td.Types {
		switch typ.Enc {
		case BytesAdaptiveEnc, StringAdaptiveEnc, ExtendedAdaptiveEnc:
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
	td.expectEncoding(i, BytesAdaptiveEnc)
	toastValue := AdaptiveValue(td.GetField(i, tup))
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
	td.expectEncoding(i, StringAdaptiveEnc)
	toastValue := AdaptiveValue(td.GetField(i, tup))
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
		return hash.New(value).String()
	case StringAddrEnc:
		return hash.New(value).String()
	case CommitAddrEnc:
		return hash.New(value).String()
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
	// If hashes are equal, the values must be equal
	if bytes.Compare(v1, v2) == 0 {
		return 0, nil
	}
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

// A AdaptiveValue is a byte sequence that can represent:
// - An inlined string or bytes value
// - An outlined (addressable) string or bytes value
// - NULL (identified by the empty sequence)
// Outlined ToastValues begin with a SQLite () variable-length encoding of the string length.
// Note that this value is always greater than 21, because we only outline values when doing so reduces the length of
// the tuple, and the outlined AdaptiveValue consists of a 20 byte address + the string length (min 1 byte).
// This means that we can use a shorter length value to indicate an inlined AdaptiveValue. We choose 0 for simplicity.
type AdaptiveValue []byte

// getMessageLength returns the length of the underlying value.
func (v AdaptiveValue) getMessageLength() int64 {
	if v.IsNull() {
		return 0
	}
	if v.isInlined() {
		return int64(len(v)) - 1
	}
	length, _ := uvarint.Uvarint(v)
	return int64(length)
}

// outlineSize computes the size of the value in the tuple if it were outlined.
func (v AdaptiveValue) outlineSize() int64 {
	if v.IsOutlined() {
		return int64(len(v))
	}
	_, lengthSize := uvarint.Uvarint(v)
	return int64(lengthSize) + 20 // variable length + address
}

// inlineSize computes the size of the value in the tuple if it were inlined.
func (v AdaptiveValue) inlineSize() int64 {
	if v.isInlined() {
		return int64(len(v))
	}
	blobLength := v.getMessageLength()
	return 1 + blobLength // header + message
}

// IsNull returns whether this AdaptiveValue represents a NULL value.
func (v AdaptiveValue) IsNull() bool {
	return len(v) == 0
}

// IsOutlined returns whether this AdaptiveValue represents a outlined addressable value.
func (v AdaptiveValue) IsOutlined() bool {
	if v.IsNull() {
		return false
	}
	return v[0] != 0
}

func makeVarInt(x uint64, dest []byte) (bytesWritten int, output []byte) {
	if dest == nil {
		dest = make([]byte, 9)
	}
	length := uvarint.Encode(dest, x)
	return length, dest[:length]
}

// If a conversion is necessary, the converted value will be copied into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v AdaptiveValue) convertToOutline(ctx context.Context, vs ValueStore, dest []byte) (AdaptiveValue, error) {
	if v.IsOutlined() {
		return v, nil
	}
	if cap(dest) < 29 {
		dest = make([]byte, 29)
	}
	blob := v[1:]
	blobLength := uint64(len(blob))
	lengthSize, dest := makeVarInt(blobLength, dest)
	blobHash, err := vs.WriteBytes(ctx, blob)
	if err != nil {
		return nil, err
	}

	dest = append(dest[:lengthSize], blobHash[:]...)
	return dest, nil
}

// isInlined returns whether this AdaptiveValue represents an inlined value.
func (v AdaptiveValue) isInlined() bool {
	if v.IsNull() {
		return false
	}
	return v[0] == 0
}

// If a conversion is necessary, the converted value will be written into `dest`. This is a performance
// optimization when there is a pre-allocated buffer.
func (v AdaptiveValue) convertToInline(ctx context.Context, vs ValueStore, dest []byte) (AdaptiveValue, error) {
	if v.isInlined() {
		return v, nil
	}
	_, lengthBytes := uvarint.Uvarint(v)
	addr := v[lengthBytes:]
	blob, err := vs.ReadBytes(ctx, hash.New(addr))
	if err != nil {
		return nil, err
	}
	outputSize := 1 + len(blob)
	if cap(dest) < outputSize {
		dest = make([]byte, outputSize)
	}
	dest = dest[:1]
	dest[0] = 0
	dest = append(dest, blob...)
	return dest, nil
}

func (v AdaptiveValue) convertToBytes(ctx context.Context, vs ValueStore, buf []byte) ([]byte, error) {
	// Only inlined values can be converted to bytes
	inlineValue, err := v.convertToInline(ctx, vs, buf)
	if err != nil {
		return nil, err
	}
	// Remove header byte
	return inlineValue[1:], nil
}

func (v AdaptiveValue) convertToByteArray(ctx context.Context, vs ValueStore, buf []byte) (*ByteArray, error) {
	// Only outlined values can be converted to a ByteArray
	outlineValue, err := v.convertToOutline(ctx, vs, buf)
	if err != nil {
		return &ByteArray{}, err
	}
	length, lengthBytes := uvarint.Uvarint(outlineValue)
	address := hash.New(outlineValue[lengthBytes:])
	return NewByteArray(address, vs).WithMaxByteLength(int64(length)), nil
}

func (v AdaptiveValue) convertToTextStorage(ctx context.Context, vs ValueStore, buf []byte) (*TextStorage, error) {
	// Only outlined values can be converted to a TextStorage
	outlineValue, err := v.convertToOutline(ctx, vs, buf)
	if err != nil {
		return &TextStorage{}, err
	}
	length, lengthBytes := uvarint.Uvarint(outlineValue)
	address := hash.New(outlineValue[lengthBytes:])
	return NewTextStorage(address, vs).WithMaxByteLength(int64(length)), nil
}

// ToastTypeHandler is an implementation of ToastTypeHandler for TOAST types, that is, values that can be either a content-address
// or an inline value. This TypeHandler converts between the address and the underlying value as needed, allowing
// these columns to be used in contexts that need access to the underlying value, such as in primary indexes.
// The |childHandler| field allows this behavior to be composed with other type handlers.
type ToastTypeHandler struct {
	vs           ValueStore
	childHandler TupleTypeHandler
}

func NewToastTypeHandler(vs ValueStore, childHandler TupleTypeHandler) ToastTypeHandler {
	return ToastTypeHandler{
		vs:           vs,
		childHandler: childHandler,
	}
}

func (handler ToastTypeHandler) SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error) {
	toastValue1 := AdaptiveValue(v1)
	toastValue2 := AdaptiveValue(v2)
	// Fast-path: two outlined values with equal hashes are equal.
	if toastValue1.IsOutlined() && toastValue2.IsOutlined() && bytes.Equal(toastValue1, toastValue2) {
		return 0, nil
	}
	var err error
	if toastValue1.IsOutlined() {
		toastValue1, err = toastValue1.convertToInline(ctx, handler.vs, nil)
		if err != nil {
			return 0, err
		}
	}
	if toastValue2.IsOutlined() {
		toastValue1, err = toastValue2.convertToInline(ctx, handler.vs, nil)
		if err != nil {
			return 0, err
		}
	}
	return handler.childHandler.SerializedCompare(ctx, toastValue1[1:], toastValue2[1:])
}

func (handler ToastTypeHandler) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	b, err := handler.childHandler.SerializeValue(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	dest := make([]byte, len(b)+1)
	copy(dest[1:], b)
	return dest, nil
}

func (handler ToastTypeHandler) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	toastVal := AdaptiveValue(val)
	if toastVal.IsNull() {
		return nil, nil
	}
	if toastVal.isInlined() {
		return handler.childHandler.DeserializeValue(ctx, toastVal[1:])
	}
	// else toastVal is outlined
	_, lengthBytes := uvarint.Uvarint(toastVal)
	addr := hash.New(toastVal[lengthBytes:])
	b, err := handler.vs.ReadBytes(ctx, addr)
	if err != nil {
		return nil, err
	}
	return handler.childHandler.DeserializeValue(ctx, b)
}

func (handler ToastTypeHandler) FormatValue(val any) (string, error) {
	return handler.childHandler.FormatValue(val)
}
