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

func (v Bool) Chunks() []Ref {
	return nil
}

func (v Bool) ChildValues() []Value {
	return nil
}

func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}

var typeForBool = BoolType

func (v Bool) Type() *Type {
	return typeForBool
}

type Float32 float32

func (p Float32) Equals(other Value) bool {
	return p == other
}

func (v Float32) Ref() ref.Ref {
	return getRef(v)
}

func (v Float32) Chunks() []Ref {
	return nil
}

func (v Float32) ChildValues() []Value {
	return nil
}

func (v Float32) ToPrimitive() interface{} {
	return float32(v)
}

var typeForFloat32 = Float32Type

func (v Float32) Type() *Type {
	return typeForFloat32
}

func (v Float32) Less(other OrderedValue) bool {
	return v < other.(Float32)
}

type Float64 float64

func (p Float64) Equals(other Value) bool {
	return p == other
}

func (v Float64) Ref() ref.Ref {
	return getRef(v)
}

func (v Float64) Chunks() []Ref {
	return nil
}

func (v Float64) ChildValues() []Value {
	return nil
}

func (v Float64) ToPrimitive() interface{} {
	return float64(v)
}

var typeForFloat64 = Float64Type

func (v Float64) Type() *Type {
	return typeForFloat64
}

func (v Float64) Less(other OrderedValue) bool {
	return v < other.(Float64)
}

type Int16 int16

func (p Int16) Equals(other Value) bool {
	return p == other
}

func (v Int16) Ref() ref.Ref {
	return getRef(v)
}

func (v Int16) Chunks() []Ref {
	return nil
}

func (v Int16) ChildValues() []Value {
	return nil
}

func (v Int16) ToPrimitive() interface{} {
	return int16(v)
}

var typeForInt16 = Int16Type

func (v Int16) Type() *Type {
	return typeForInt16
}

func (v Int16) Less(other OrderedValue) bool {
	return v < other.(Int16)
}

type Int32 int32

func (p Int32) Equals(other Value) bool {
	return p == other
}

func (v Int32) Ref() ref.Ref {
	return getRef(v)
}

func (v Int32) Chunks() []Ref {
	return nil
}

func (v Int32) ChildValues() []Value {
	return nil
}

func (v Int32) ToPrimitive() interface{} {
	return int32(v)
}

var typeForInt32 = Int32Type

func (v Int32) Type() *Type {
	return typeForInt32
}

func (v Int32) Less(other OrderedValue) bool {
	return v < other.(Int32)
}

type Int64 int64

func (p Int64) Equals(other Value) bool {
	return p == other
}

func (v Int64) Ref() ref.Ref {
	return getRef(v)
}

func (v Int64) Chunks() []Ref {
	return nil
}

func (v Int64) ChildValues() []Value {
	return nil
}

func (v Int64) ToPrimitive() interface{} {
	return int64(v)
}

var typeForInt64 = Int64Type

func (v Int64) Type() *Type {
	return typeForInt64
}

func (v Int64) Less(other OrderedValue) bool {
	return v < other.(Int64)
}

type Int8 int8

func (p Int8) Equals(other Value) bool {
	return p == other
}

func (v Int8) Ref() ref.Ref {
	return getRef(v)
}

func (v Int8) Chunks() []Ref {
	return nil
}

func (v Int8) ChildValues() []Value {
	return nil
}

func (v Int8) ToPrimitive() interface{} {
	return int8(v)
}

var typeForInt8 = Int8Type

func (v Int8) Type() *Type {
	return typeForInt8
}

func (v Int8) Less(other OrderedValue) bool {
	return v < other.(Int8)
}

type Uint16 uint16

func (p Uint16) Equals(other Value) bool {
	return p == other
}

func (v Uint16) Ref() ref.Ref {
	return getRef(v)
}

func (v Uint16) Chunks() []Ref {
	return nil
}

func (v Uint16) ChildValues() []Value {
	return nil
}

func (v Uint16) ToPrimitive() interface{} {
	return uint16(v)
}

var typeForUint16 = Uint16Type

func (v Uint16) Type() *Type {
	return typeForUint16
}

func (v Uint16) Less(other OrderedValue) bool {
	return v < other.(Uint16)
}

type Uint32 uint32

func (p Uint32) Equals(other Value) bool {
	return p == other
}

func (v Uint32) Ref() ref.Ref {
	return getRef(v)
}

func (v Uint32) Chunks() []Ref {
	return nil
}

func (v Uint32) ChildValues() []Value {
	return nil
}

func (v Uint32) ToPrimitive() interface{} {
	return uint32(v)
}

var typeForUint32 = Uint32Type

func (v Uint32) Type() *Type {
	return typeForUint32
}

func (v Uint32) Less(other OrderedValue) bool {
	return v < other.(Uint32)
}

type Uint64 uint64

func (p Uint64) Equals(other Value) bool {
	return p == other
}

func (v Uint64) Ref() ref.Ref {
	return getRef(v)
}

func (v Uint64) Chunks() []Ref {
	return nil
}

func (v Uint64) ChildValues() []Value {
	return nil
}

func (v Uint64) ToPrimitive() interface{} {
	return uint64(v)
}

var typeForUint64 = Uint64Type

func (v Uint64) Type() *Type {
	return typeForUint64
}

func (v Uint64) Less(other OrderedValue) bool {
	return v < other.(Uint64)
}

type Uint8 uint8

func (p Uint8) Equals(other Value) bool {
	return p == other
}

func (v Uint8) Ref() ref.Ref {
	return getRef(v)
}

func (v Uint8) Chunks() []Ref {
	return nil
}

func (v Uint8) ChildValues() []Value {
	return nil
}

func (v Uint8) ToPrimitive() interface{} {
	return uint8(v)
}

var typeForUint8 = Uint8Type

func (v Uint8) Type() *Type {
	return typeForUint8
}

func (v Uint8) Less(other OrderedValue) bool {
	return v < other.(Uint8)
}

