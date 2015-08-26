// DO NOT EDIT: This file was generated.
// To regenerate, run `go generate` in this package.

package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

func (self Bool) Equals(other Value) bool {
	if other, ok := other.(Bool); ok {
		return self == other
	} else {
		return false
	}
}

func (v Bool) Ref() ref.Ref {
	return getRef(v)
}

func (v Bool) Chunks() []Future {
	return nil
}

func BoolFromVal(v Value) Bool {
	return v.(Bool)
}

func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}

type Int8 int8

func (self Int8) Equals(other Value) bool {
	if other, ok := other.(Int8); ok {
		return self == other
	} else {
		return false
	}
}

func (v Int8) Ref() ref.Ref {
	return getRef(v)
}

func (v Int8) Chunks() []Future {
	return nil
}

func Int8FromVal(v Value) Int8 {
	return v.(Int8)
}

func (v Int8) ToPrimitive() interface{} {
	return int8(v)
}

type Int16 int16

func (self Int16) Equals(other Value) bool {
	if other, ok := other.(Int16); ok {
		return self == other
	} else {
		return false
	}
}

func (v Int16) Ref() ref.Ref {
	return getRef(v)
}

func (v Int16) Chunks() []Future {
	return nil
}

func Int16FromVal(v Value) Int16 {
	return v.(Int16)
}

func (v Int16) ToPrimitive() interface{} {
	return int16(v)
}

type Int32 int32

func (self Int32) Equals(other Value) bool {
	if other, ok := other.(Int32); ok {
		return self == other
	} else {
		return false
	}
}

func (v Int32) Ref() ref.Ref {
	return getRef(v)
}

func (v Int32) Chunks() []Future {
	return nil
}

func Int32FromVal(v Value) Int32 {
	return v.(Int32)
}

func (v Int32) ToPrimitive() interface{} {
	return int32(v)
}

type Int64 int64

func (self Int64) Equals(other Value) bool {
	if other, ok := other.(Int64); ok {
		return self == other
	} else {
		return false
	}
}

func (v Int64) Ref() ref.Ref {
	return getRef(v)
}

func (v Int64) Chunks() []Future {
	return nil
}

func Int64FromVal(v Value) Int64 {
	return v.(Int64)
}

func (v Int64) ToPrimitive() interface{} {
	return int64(v)
}

type UInt8 uint8

func (self UInt8) Equals(other Value) bool {
	if other, ok := other.(UInt8); ok {
		return self == other
	} else {
		return false
	}
}

func (v UInt8) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt8) Chunks() []Future {
	return nil
}

func UInt8FromVal(v Value) UInt8 {
	return v.(UInt8)
}

func (v UInt8) ToPrimitive() interface{} {
	return uint8(v)
}

type UInt16 uint16

func (self UInt16) Equals(other Value) bool {
	if other, ok := other.(UInt16); ok {
		return self == other
	} else {
		return false
	}
}

func (v UInt16) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt16) Chunks() []Future {
	return nil
}

func UInt16FromVal(v Value) UInt16 {
	return v.(UInt16)
}

func (v UInt16) ToPrimitive() interface{} {
	return uint16(v)
}

type UInt32 uint32

func (self UInt32) Equals(other Value) bool {
	if other, ok := other.(UInt32); ok {
		return self == other
	} else {
		return false
	}
}

func (v UInt32) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt32) Chunks() []Future {
	return nil
}

func UInt32FromVal(v Value) UInt32 {
	return v.(UInt32)
}

func (v UInt32) ToPrimitive() interface{} {
	return uint32(v)
}

type UInt64 uint64

func (self UInt64) Equals(other Value) bool {
	if other, ok := other.(UInt64); ok {
		return self == other
	} else {
		return false
	}
}

func (v UInt64) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt64) Chunks() []Future {
	return nil
}

func UInt64FromVal(v Value) UInt64 {
	return v.(UInt64)
}

func (v UInt64) ToPrimitive() interface{} {
	return uint64(v)
}

type Float32 float32

func (self Float32) Equals(other Value) bool {
	if other, ok := other.(Float32); ok {
		return self == other
	} else {
		return false
	}
}

func (v Float32) Ref() ref.Ref {
	return getRef(v)
}

func (v Float32) Chunks() []Future {
	return nil
}

func Float32FromVal(v Value) Float32 {
	return v.(Float32)
}

func (v Float32) ToPrimitive() interface{} {
	return float32(v)
}

type Float64 float64

func (self Float64) Equals(other Value) bool {
	if other, ok := other.(Float64); ok {
		return self == other
	} else {
		return false
	}
}

func (v Float64) Ref() ref.Ref {
	return getRef(v)
}

func (v Float64) Chunks() []Future {
	return nil
}

func Float64FromVal(v Value) Float64 {
	return v.(Float64)
}

func (v Float64) ToPrimitive() interface{} {
	return float64(v)
}

