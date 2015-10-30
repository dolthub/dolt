// DO NOT EDIT: This file was generated.
// To regenerate, run `go generate` in this package.

package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

func (p Bool) Equals(other Value) bool {
	return other != nil && typeRefForBool.Equals(other.TypeRef()) && p == other
}

func (v Bool) Ref() ref.Ref {
	return getRef(v)
}

func (v Bool) Chunks() []ref.Ref {
	return nil
}

func BoolFromVal(v Value) Bool {
	return v.(Bool)
}

func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}

var typeRefForBool = MakePrimitiveTypeRef(BoolKind)

func (v Bool) TypeRef() TypeRef {
	return typeRefForBool
}

type Int8 int8

func (p Int8) Equals(other Value) bool {
	return other != nil && typeRefForInt8.Equals(other.TypeRef()) && p == other
}

func (v Int8) Ref() ref.Ref {
	return getRef(v)
}

func (v Int8) Chunks() []ref.Ref {
	return nil
}

func Int8FromVal(v Value) Int8 {
	return v.(Int8)
}

func (v Int8) ToPrimitive() interface{} {
	return int8(v)
}

var typeRefForInt8 = MakePrimitiveTypeRef(Int8Kind)

func (v Int8) TypeRef() TypeRef {
	return typeRefForInt8
}

type Int16 int16

func (p Int16) Equals(other Value) bool {
	return other != nil && typeRefForInt16.Equals(other.TypeRef()) && p == other
}

func (v Int16) Ref() ref.Ref {
	return getRef(v)
}

func (v Int16) Chunks() []ref.Ref {
	return nil
}

func Int16FromVal(v Value) Int16 {
	return v.(Int16)
}

func (v Int16) ToPrimitive() interface{} {
	return int16(v)
}

var typeRefForInt16 = MakePrimitiveTypeRef(Int16Kind)

func (v Int16) TypeRef() TypeRef {
	return typeRefForInt16
}

type Int32 int32

func (p Int32) Equals(other Value) bool {
	return other != nil && typeRefForInt32.Equals(other.TypeRef()) && p == other
}

func (v Int32) Ref() ref.Ref {
	return getRef(v)
}

func (v Int32) Chunks() []ref.Ref {
	return nil
}

func Int32FromVal(v Value) Int32 {
	return v.(Int32)
}

func (v Int32) ToPrimitive() interface{} {
	return int32(v)
}

var typeRefForInt32 = MakePrimitiveTypeRef(Int32Kind)

func (v Int32) TypeRef() TypeRef {
	return typeRefForInt32
}

type Int64 int64

func (p Int64) Equals(other Value) bool {
	return other != nil && typeRefForInt64.Equals(other.TypeRef()) && p == other
}

func (v Int64) Ref() ref.Ref {
	return getRef(v)
}

func (v Int64) Chunks() []ref.Ref {
	return nil
}

func Int64FromVal(v Value) Int64 {
	return v.(Int64)
}

func (v Int64) ToPrimitive() interface{} {
	return int64(v)
}

var typeRefForInt64 = MakePrimitiveTypeRef(Int64Kind)

func (v Int64) TypeRef() TypeRef {
	return typeRefForInt64
}

type UInt8 uint8

func (p UInt8) Equals(other Value) bool {
	return other != nil && typeRefForUInt8.Equals(other.TypeRef()) && p == other
}

func (v UInt8) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt8) Chunks() []ref.Ref {
	return nil
}

func UInt8FromVal(v Value) UInt8 {
	return v.(UInt8)
}

func (v UInt8) ToPrimitive() interface{} {
	return uint8(v)
}

var typeRefForUInt8 = MakePrimitiveTypeRef(UInt8Kind)

func (v UInt8) TypeRef() TypeRef {
	return typeRefForUInt8
}

type UInt16 uint16

func (p UInt16) Equals(other Value) bool {
	return other != nil && typeRefForUInt16.Equals(other.TypeRef()) && p == other
}

func (v UInt16) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt16) Chunks() []ref.Ref {
	return nil
}

func UInt16FromVal(v Value) UInt16 {
	return v.(UInt16)
}

func (v UInt16) ToPrimitive() interface{} {
	return uint16(v)
}

var typeRefForUInt16 = MakePrimitiveTypeRef(UInt16Kind)

func (v UInt16) TypeRef() TypeRef {
	return typeRefForUInt16
}

type UInt32 uint32

func (p UInt32) Equals(other Value) bool {
	return other != nil && typeRefForUInt32.Equals(other.TypeRef()) && p == other
}

func (v UInt32) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt32) Chunks() []ref.Ref {
	return nil
}

func UInt32FromVal(v Value) UInt32 {
	return v.(UInt32)
}

func (v UInt32) ToPrimitive() interface{} {
	return uint32(v)
}

var typeRefForUInt32 = MakePrimitiveTypeRef(UInt32Kind)

func (v UInt32) TypeRef() TypeRef {
	return typeRefForUInt32
}

type UInt64 uint64

func (p UInt64) Equals(other Value) bool {
	return other != nil && typeRefForUInt64.Equals(other.TypeRef()) && p == other
}

func (v UInt64) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt64) Chunks() []ref.Ref {
	return nil
}

func UInt64FromVal(v Value) UInt64 {
	return v.(UInt64)
}

func (v UInt64) ToPrimitive() interface{} {
	return uint64(v)
}

var typeRefForUInt64 = MakePrimitiveTypeRef(UInt64Kind)

func (v UInt64) TypeRef() TypeRef {
	return typeRefForUInt64
}

type Float32 float32

func (p Float32) Equals(other Value) bool {
	return other != nil && typeRefForFloat32.Equals(other.TypeRef()) && p == other
}

func (v Float32) Ref() ref.Ref {
	return getRef(v)
}

func (v Float32) Chunks() []ref.Ref {
	return nil
}

func Float32FromVal(v Value) Float32 {
	return v.(Float32)
}

func (v Float32) ToPrimitive() interface{} {
	return float32(v)
}

var typeRefForFloat32 = MakePrimitiveTypeRef(Float32Kind)

func (v Float32) TypeRef() TypeRef {
	return typeRefForFloat32
}

type Float64 float64

func (p Float64) Equals(other Value) bool {
	return other != nil && typeRefForFloat64.Equals(other.TypeRef()) && p == other
}

func (v Float64) Ref() ref.Ref {
	return getRef(v)
}

func (v Float64) Chunks() []ref.Ref {
	return nil
}

func Float64FromVal(v Value) Float64 {
	return v.(Float64)
}

func (v Float64) ToPrimitive() interface{} {
	return float64(v)
}

var typeRefForFloat64 = MakePrimitiveTypeRef(Float64Kind)

func (v Float64) TypeRef() TypeRef {
	return typeRefForFloat64
}

