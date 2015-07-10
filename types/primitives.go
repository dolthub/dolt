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

