package types

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func fromTypedEncodeable(i []interface{}, vr ValueReader) Value {
	r := newJSONArrayReader(i, vr)
	return r.readTopLevelValue()
}

type jsonArrayReader struct {
	a  []interface{}
	i  int
	vr ValueReader
}

func newJSONArrayReader(a []interface{}, vr ValueReader) *jsonArrayReader {
	return &jsonArrayReader{a: a, i: 0, vr: vr}
}

func (r *jsonArrayReader) read() interface{} {
	v := r.a[r.i]
	r.i++
	return v
}

func (r *jsonArrayReader) atEnd() bool {
	return r.i >= len(r.a)
}

func (r *jsonArrayReader) readString() string {
	return r.read().(string)
}

func (r *jsonArrayReader) readBool() bool {
	return r.read().(bool)
}

func (r *jsonArrayReader) readFloat() float64 {
	v, err := strconv.ParseFloat(r.readString(), 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readInt() int64 {
	v, err := strconv.ParseInt(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readUint() uint64 {
	v, err := strconv.ParseUint(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readUint8() uint8 {
	return uint8(r.read().(float64))
}

func (r *jsonArrayReader) readArray() []interface{} {
	return r.read().([]interface{})
}

func (r *jsonArrayReader) readKind() NomsKind {
	return NomsKind(r.read().(float64))
}

func (r *jsonArrayReader) readRef() ref.Ref {
	s := r.readString()
	return ref.Parse(s)
}

func (r *jsonArrayReader) readTypeAsTag(backRefs []*Type) *Type {
	kind := r.readKind()
	switch kind {
	case ListKind:
		elemType := r.readTypeAsTag(backRefs)
		return MakeListType(elemType)
	case SetKind:
		elemType := r.readTypeAsTag(backRefs)
		return MakeSetType(elemType)
	case RefKind:
		elemType := r.readTypeAsTag(backRefs)
		return MakeRefType(elemType)
	case MapKind:
		keyType := r.readTypeAsTag(backRefs)
		valueType := r.readTypeAsTag(backRefs)
		return MakeMapType(keyType, valueType)
	case TypeKind:
		return TypeType
	case StructKind:
		return r.readStructType(backRefs)
	case BackRefKind:
		i := r.readUint8()
		d.Chk.True(i < uint8(len(backRefs)))
		return backRefs[len(backRefs)-1-int(i)]
	}

	if IsPrimitiveKind(kind) {
		return MakePrimitiveType(kind)
	}

	panic("unreachable")
}

func (r *jsonArrayReader) readBlob() Value {
	s := r.readString()
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(s))
	b, err := ioutil.ReadAll(decoder)
	d.Exp.NoError(err)
	return newBlobLeaf(b)
}

func (r *jsonArrayReader) readList(t *Type) Value {
	desc := t.Desc.(CompoundDesc)
	data := []Value{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType)
		data = append(data, v)
	}

	return newListLeaf(t, data...)
}

func (r *jsonArrayReader) readSet(t *Type) Value {
	desc := t.Desc.(CompoundDesc)
	data := setData{}
	elemType := desc.ElemTypes[0]
	for !r.atEnd() {
		v := r.readValueWithoutTag(elemType)
		data = append(data, v)
	}

	return newSetLeaf(t, data...)
}

func (r *jsonArrayReader) readMap(t *Type) Value {
	desc := t.Desc.(CompoundDesc)
	data := mapData{}
	keyType := desc.ElemTypes[0]
	valueType := desc.ElemTypes[1]

	for !r.atEnd() {
		k := r.readValueWithoutTag(keyType)
		v := r.readValueWithoutTag(valueType)
		data = append(data, mapEntry{k, v})
	}

	return newMapLeaf(t, data...)
}

func indexTypeForMetaSequence(t *Type) *Type {
	switch t.Kind() {
	default:
		panic(fmt.Sprintf("Unknown type used for metaSequence: %s", t.Describe()))
	case BlobKind, ListKind:
		return NumberType
	case MapKind, SetKind:
		elemType := t.Desc.(CompoundDesc).ElemTypes[0]
		if elemType.IsOrdered() {
			return elemType
		}
		return MakeRefType(ValueType)
	}
}

func (r *jsonArrayReader) maybeReadMetaSequence(t *Type) (Value, bool) {
	if !r.read().(bool) {
		return nil, false
	}

	r2 := newJSONArrayReader(r.readArray(), r.vr)
	data := metaSequenceData{}
	indexType := indexTypeForMetaSequence(t)
	for !r2.atEnd() {
		ref := NewTypedRef(MakeRefType(t), r2.readRef())
		v := r2.readValueWithoutTag(indexType)
		numLeaves := uint64(r2.readUint())
		data = append(data, newMetaTuple(v, nil, ref, numLeaves))
	}

	return newMetaSequenceFromData(data, t, r.vr), true
}

func (r *jsonArrayReader) readRefValue(t *Type) Value {
	ref := r.readRef()
	return NewTypedRef(t, ref)
}

func (r *jsonArrayReader) readTopLevelValue() Value {
	t := r.readTypeAsTag(nil)
	return r.readValueWithoutTag(t)
}

func (r *jsonArrayReader) readValueWithoutTag(t *Type) Value {
	switch t.Kind() {
	case BlobKind:
		if ms, ok := r.maybeReadMetaSequence(t); ok {
			return ms
		}

		return r.readBlob()
	case BoolKind:
		return Bool(r.read().(bool))
	case NumberKind:
		return Number(r.readFloat())
	case StringKind:
		return NewString(r.readString())
	case ValueKind:
		// The value is always tagged
		t := r.readTypeAsTag(nil)
		return r.readValueWithoutTag(t)
	case ListKind:
		if ms, ok := r.maybeReadMetaSequence(t); ok {
			return ms
		}

		r2 := newJSONArrayReader(r.readArray(), r.vr)
		return r2.readList(t)
	case MapKind:
		if ms, ok := r.maybeReadMetaSequence(t); ok {
			return ms
		}

		r2 := newJSONArrayReader(r.readArray(), r.vr)
		return r2.readMap(t)
	case RefKind:
		return r.readRefValue(t)
	case SetKind:
		if ms, ok := r.maybeReadMetaSequence(t); ok {
			return ms
		}

		r2 := newJSONArrayReader(r.readArray(), r.vr)
		return r2.readSet(t)
	case StructKind:
		return r.readStruct(t)
	case TypeKind:
		return r.readTypeKindToValue(t)
	case BackRefKind:
		panic("BackRefKind should have been replaced")
	}

	panic("not reachable")
}

func (r *jsonArrayReader) readTypeKindToValue(t *Type) Value {
	d.Chk.IsType(PrimitiveDesc(0), t.Desc)
	return r.readTypeAsValue(nil)
}

func (r *jsonArrayReader) readTypeAsValue(backRefs []*Type) *Type {
	k := r.readKind()
	switch k {
	case ListKind, MapKind, RefKind, SetKind:
		r2 := newJSONArrayReader(r.readArray(), r.vr)
		elemTypes := []*Type{}
		for !r2.atEnd() {
			t := r2.readTypeAsValue(backRefs)
			elemTypes = append(elemTypes, t)
		}
		return makeCompoundType(k, elemTypes...)
	case StructKind:
		return r.readStructType(backRefs)
	}

	d.Chk.True(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *jsonArrayReader) readStruct(t *Type) Value {
	// We've read `[StructKind, name, fields, unions` at this point
	values := []Value{}
	desc := t.Desc.(StructDesc)
	for _, f := range desc.Fields {
		values = append(values, r.readValueWithoutTag(f.T))
	}

	return structBuilder(values, t)
}

func (r *jsonArrayReader) readStructType(backRefs []*Type) *Type {
	name := r.readString()

	fields := []Field{}
	st := MakeStructType(name, fields)
	backRefs = append(backRefs, st)
	desc := st.Desc.(StructDesc)

	fieldReader := newJSONArrayReader(r.readArray(), r.vr)
	for !fieldReader.atEnd() {
		fieldName := fieldReader.readString()
		fieldType := fieldReader.readTypeAsTag(backRefs)
		fields = append(fields, Field{Name: fieldName, T: fieldType})
	}
	desc.Fields = fields
	st.Desc = desc
	return st
}
