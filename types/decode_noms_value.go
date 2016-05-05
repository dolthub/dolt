package types

import (
	"encoding/base64"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func fromTypedEncodeable(i []interface{}, vr ValueReader) Value {
	r := newJSONArrayReader(i, vr)
	return r.readValue()
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

func (r *jsonArrayReader) readRef(t *Type) Ref {
	ref := ref.Parse(r.readString())
	height := r.readUint()
	return NewTypedRef(t, ref, height)
}

func (r *jsonArrayReader) readType(parentStructTypes []*Type) *Type {
	k := r.readKind()
	switch k {
	case ListKind:
		return MakeListType(r.readType(parentStructTypes))
	case MapKind:
		return MakeMapType(r.readType(parentStructTypes), r.readType(parentStructTypes))
	case RefKind:
		return MakeRefType(r.readType(parentStructTypes))
	case SetKind:
		return MakeSetType(r.readType(parentStructTypes))
	case StructKind:
		return r.readStructType(parentStructTypes)
	case ParentKind:
		i := r.readUint8()
		d.Chk.True(i < uint8(len(parentStructTypes)))
		return parentStructTypes[len(parentStructTypes)-1-int(i)]
	}

	d.Chk.True(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *jsonArrayReader) readBlob() Value {
	s := r.readString()
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(s))
	b, err := ioutil.ReadAll(decoder)
	d.Exp.NoError(err)
	return newBlobLeaf(b)
}

func (r *jsonArrayReader) readList(t *Type) Value {
	data := []Value{}
	for !r.atEnd() {
		v := r.readValue()
		data = append(data, v)
	}

	return newListLeaf(t, data...)
}

func (r *jsonArrayReader) readSet(t *Type) Value {
	data := setData{}
	for !r.atEnd() {
		v := r.readValue()
		data = append(data, v)
	}

	return newSetLeaf(t, data...)
}

func (r *jsonArrayReader) readMap(t *Type) Value {
	data := mapData{}
	for !r.atEnd() {
		k := r.readValue()
		v := r.readValue()
		data = append(data, mapEntry{k, v})
	}

	return newMapLeaf(t, data...)
}

func (r *jsonArrayReader) maybeReadMetaSequence(t *Type) (Value, bool) {
	if !r.read().(bool) {
		return nil, false
	}

	r2 := newJSONArrayReader(r.readArray(), r.vr)
	data := metaSequenceData{}
	for !r2.atEnd() {
		ref := r2.readRef(MakeRefType(t))
		v := r2.readValue()
		numLeaves := uint64(r2.readUint())
		data = append(data, newMetaTuple(v, nil, ref, numLeaves))
	}

	return newMetaSequenceFromData(data, t, r.vr), true
}

func (r *jsonArrayReader) readValue() Value {
	t := r.readType(nil)
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
		return r.readValue()
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
		return r.readRef(t)
	case SetKind:
		if ms, ok := r.maybeReadMetaSequence(t); ok {
			return ms
		}

		r2 := newJSONArrayReader(r.readArray(), r.vr)
		return r2.readSet(t)
	case StructKind:
		return r.readStruct(t)
	case TypeKind:
		return r.readType(nil)
	case ParentKind:
		panic("ParentKind should have been replaced")
	}

	panic("not reachable")
}

func (r *jsonArrayReader) readStruct(t *Type) Value {
	// We've read `[StructKind, name, fields, unions` at this point
	values := []Value{}
	desc := t.Desc.(StructDesc)
	desc.IterFields(func(name string, t *Type) {
		values = append(values, r.readValue())
	})

	return structBuilder(values, t)
}

func (r *jsonArrayReader) readStructType(parentStructTypes []*Type) *Type {
	name := r.readString()

	fields := map[string]*Type{}
	st := MakeStructType(name, fields)
	parentStructTypes = append(parentStructTypes, st)
	desc := st.Desc.(StructDesc)

	fieldReader := newJSONArrayReader(r.readArray(), r.vr)
	for !fieldReader.atEnd() {
		fieldName := fieldReader.readString()
		fieldType := fieldReader.readType(parentStructTypes)
		fields[fieldName] = fieldType
	}
	desc.Fields = fields
	st.Desc = desc
	return st
}
