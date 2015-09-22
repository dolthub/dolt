package test

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/types"
)

func TestAccessors(t *testing.T) {
	assert := assert.New(t)

	def := StructPrimitivesDef{
		Uint64:  uint64(1),
		Uint32:  uint32(2),
		Uint16:  uint16(3),
		Uint8:   uint8(4),
		Int64:   int64(5),
		Int32:   int32(6),
		Int16:   int16(7),
		Int8:    int8(8),
		Float64: float64(9),
		Float32: float32(10),
		Bool:    bool(false),
		String:  string("hi"),
		Blob:    types.NewEmptyBlob(),
		Value:   types.Bool(false),
	}

	st := def.New()

	assert.Equal(uint64(1), st.Uint64())
	st.SetUint64(uint64(11))
	assert.Equal(uint64(1), st.Uint64())
	st = st.SetUint64(uint64(11))
	assert.Equal(uint64(11), st.Uint64())

	assert.Equal(uint32(2), st.Uint32())
	st.SetUint32(uint32(22))
	assert.Equal(uint32(2), st.Uint32())
	st = st.SetUint32(uint32(22))
	assert.Equal(uint32(22), st.Uint32())

	assert.Equal(uint16(3), st.Uint16())
	st.SetUint16(uint16(33))
	assert.Equal(uint16(3), st.Uint16())
	st = st.SetUint16(uint16(33))
	assert.Equal(uint16(33), st.Uint16())

	assert.Equal(uint8(4), st.Uint8())
	st.SetUint8(uint8(44))
	assert.Equal(uint8(4), st.Uint8())
	st = st.SetUint8(uint8(44))
	assert.Equal(uint8(44), st.Uint8())

	assert.Equal(int64(5), st.Int64())
	st.SetInt64(int64(55))
	assert.Equal(int64(5), st.Int64())
	st = st.SetInt64(int64(55))
	assert.Equal(int64(55), st.Int64())

	assert.Equal(int32(6), st.Int32())
	st.SetInt32(int32(66))
	assert.Equal(int32(6), st.Int32())
	st = st.SetInt32(int32(66))
	assert.Equal(int32(66), st.Int32())

	assert.Equal(int16(7), st.Int16())
	st.SetInt16(int16(77))
	assert.Equal(int16(7), st.Int16())
	st = st.SetInt16(int16(77))
	assert.Equal(int16(77), st.Int16())

	assert.Equal(int8(8), st.Int8())
	st.SetInt8(int8(88))
	assert.Equal(int8(8), st.Int8())
	st = st.SetInt8(int8(88))
	assert.Equal(int8(88), st.Int8())

	assert.Equal(float64(9), st.Float64())
	st.SetFloat64(float64(99))
	assert.Equal(float64(9), st.Float64())
	st = st.SetFloat64(float64(99))
	assert.Equal(float64(99), st.Float64())

	assert.Equal(float32(10), st.Float32())
	st.SetFloat32(float32(1010))
	assert.Equal(float32(10), st.Float32())
	st = st.SetFloat32(float32(1010))
	assert.Equal(float32(1010), st.Float32())

	assert.Equal(false, st.Bool())
	st.SetBool(true)
	assert.Equal(false, st.Bool())
	st = st.SetBool(true)
	assert.Equal(true, st.Bool())

	assert.Equal("hi", st.String())
	st.SetString("bye")
	assert.Equal("hi", st.String())
	st = st.SetString("bye")
	assert.Equal("bye", st.String())

	assert.True(st.Blob().Equals(types.NewEmptyBlob()))
	b, err := types.NewBlob(strings.NewReader("hello"))
	assert.NoError(err)
	st.SetBlob(b)
	assert.True(st.Blob().Equals(types.NewEmptyBlob()))
	st = st.SetBlob(b)
	assert.True(st.Blob().Equals(b))

	assert.True(st.Value().Equals(types.Bool(false)))
	st.SetValue(types.NewString("x"))
	assert.True(st.Value().Equals(types.Bool(false)))
	st = st.SetValue(types.NewString("x"))
	assert.True(st.Value().Equals(types.NewString("x")))
}

func TestStructBackingMapKeyNames(t *testing.T) {
	assert := assert.New(t)

	s := NewStructPrimitives().SetBool(true)
	assert.True(bool(s.NomsValue().(types.Map).Get(types.NewString("bool")).(types.Bool)))
}
