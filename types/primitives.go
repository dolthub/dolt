// DO NOT EDIT: This file was generated.
// To regenerate, run `go generate` in this package.

package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

func (p Bool) Equals(other Value) bool {
	return p == other
}

func (v Bool) Ref() ref.Ref {
	return getRef(v)
}

func (v Bool) Chunks() []ref.Ref {
	return nil
}

func (v Bool) ChildValues() []Value {
	return nil
}

func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}

var typeRefForBool = MakePrimitiveTypeRef(BoolKind)

func (v Bool) Type() Type {
	return typeRefForBool
}

type Int8 int8

func (p Int8) Equals(other Value) bool {
	return p == other
}

func (v Int8) Ref() ref.Ref {
	return getRef(v)
}

func (v Int8) Chunks() []ref.Ref {
	return nil
}

func (v Int8) ChildValues() []Value {
	return nil
}

func (v Int8) ToPrimitive() interface{} {
	return int8(v)
}

var typeRefForInt8 = MakePrimitiveTypeRef(Int8Kind)

func (v Int8) Type() Type {
	return typeRefForInt8
}

type Int16 int16

func (p Int16) Equals(other Value) bool {
	return p == other
}

func (v Int16) Ref() ref.Ref {
	return getRef(v)
}

func (v Int16) Chunks() []ref.Ref {
	return nil
}

func (v Int16) ChildValues() []Value {
	return nil
}

func (v Int16) ToPrimitive() interface{} {
	return int16(v)
}

var typeRefForInt16 = MakePrimitiveTypeRef(Int16Kind)

func (v Int16) Type() Type {
	return typeRefForInt16
}

type Int32 int32

func (p Int32) Equals(other Value) bool {
	return p == other
}

func (v Int32) Ref() ref.Ref {
	return getRef(v)
}

func (v Int32) Chunks() []ref.Ref {
	return nil
}

func (v Int32) ChildValues() []Value {
	return nil
}

func (v Int32) ToPrimitive() interface{} {
	return int32(v)
}

var typeRefForInt32 = MakePrimitiveTypeRef(Int32Kind)

func (v Int32) Type() Type {
	return typeRefForInt32
}

type Int64 int64

func (p Int64) Equals(other Value) bool {
	return p == other
}

func (v Int64) Ref() ref.Ref {
	return getRef(v)
}

func (v Int64) Chunks() []ref.Ref {
	return nil
}

func (v Int64) ChildValues() []Value {
	return nil
}

func (v Int64) ToPrimitive() interface{} {
	return int64(v)
}

var typeRefForInt64 = MakePrimitiveTypeRef(Int64Kind)

func (v Int64) Type() Type {
	return typeRefForInt64
}

type UInt8 uint8

func (p UInt8) Equals(other Value) bool {
	return p == other
}

func (v UInt8) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt8) Chunks() []ref.Ref {
	return nil
}

func (v UInt8) ChildValues() []Value {
	return nil
}

func (v UInt8) ToPrimitive() interface{} {
	return uint8(v)
}

var typeRefForUInt8 = MakePrimitiveTypeRef(UInt8Kind)

func (v UInt8) Type() Type {
	return typeRefForUInt8
}

type UInt16 uint16

func (p UInt16) Equals(other Value) bool {
	return p == other
}

func (v UInt16) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt16) Chunks() []ref.Ref {
	return nil
}

func (v UInt16) ChildValues() []Value {
	return nil
}

func (v UInt16) ToPrimitive() interface{} {
	return uint16(v)
}

var typeRefForUInt16 = MakePrimitiveTypeRef(UInt16Kind)

func (v UInt16) Type() Type {
	return typeRefForUInt16
}

type UInt32 uint32

func (p UInt32) Equals(other Value) bool {
	return p == other
}

func (v UInt32) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt32) Chunks() []ref.Ref {
	return nil
}

func (v UInt32) ChildValues() []Value {
	return nil
}

func (v UInt32) ToPrimitive() interface{} {
	return uint32(v)
}

var typeRefForUInt32 = MakePrimitiveTypeRef(UInt32Kind)

func (v UInt32) Type() Type {
	return typeRefForUInt32
}

type UInt64 uint64

func (p UInt64) Equals(other Value) bool {
	return p == other
}

func (v UInt64) Ref() ref.Ref {
	return getRef(v)
}

func (v UInt64) Chunks() []ref.Ref {
	return nil
}

func (v UInt64) ChildValues() []Value {
	return nil
}

func (v UInt64) ToPrimitive() interface{} {
	return uint64(v)
}

var typeRefForUInt64 = MakePrimitiveTypeRef(UInt64Kind)

func (v UInt64) Type() Type {
	return typeRefForUInt64
}

type Float32 float32

func (p Float32) Equals(other Value) bool {
	return p == other
}

func (v Float32) Ref() ref.Ref {
	return getRef(v)
}

func (v Float32) Chunks() []ref.Ref {
	return nil
}

func (v Float32) ChildValues() []Value {
	return nil
}

func (v Float32) ToPrimitive() interface{} {
	return float32(v)
}

var typeRefForFloat32 = MakePrimitiveTypeRef(Float32Kind)

func (v Float32) Type() Type {
	return typeRefForFloat32
}

type Float64 float64

func (p Float64) Equals(other Value) bool {
	return p == other
}

func (v Float64) Ref() ref.Ref {
	return getRef(v)
}

func (v Float64) Chunks() []ref.Ref {
	return nil
}

func (v Float64) ChildValues() []Value {
	return nil
}

func (v Float64) ToPrimitive() interface{} {
	return float64(v)
}

var typeRefForFloat64 = MakePrimitiveTypeRef(Float64Kind)

func (v Float64) Type() Type {
	return typeRefForFloat64
}

