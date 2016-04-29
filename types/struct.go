package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type structData map[string]Value

type Struct struct {
	data structData
	t    *Type
	ref  *ref.Ref
}

func newStructFromData(data structData, t *Type) Struct {
	d.Chk.Equal(t.Kind(), StructKind)
	return Struct{data, t, &ref.Ref{}}
}

func NewStruct(t *Type, data structData) Struct {
	newData := make(structData)
	desc := t.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := data[f.Name]
		d.Chk.True(ok, "Missing required field %s", f.Name)
		newData[f.Name] = v
	}
	return newStructFromData(newData, t)
}

func (s Struct) Equals(other Value) bool {
	return other != nil && s.t.Equals(other.Type()) && s.Ref() == other.Ref()
}

func (s Struct) Ref() ref.Ref {
	return EnsureRef(s.ref, s)
}

func (s Struct) Chunks() (chunks []Ref) {
	chunks = append(chunks, s.t.Chunks()...)
	for _, f := range s.desc().Fields {
		v, ok := s.data[f.Name]
		d.Chk.True(ok)
		chunks = append(chunks, v.Chunks()...)
	}

	return
}

func (s Struct) ChildValues() (res []Value) {
	res = append(res, s.t)
	for _, f := range s.desc().Fields {
		v, ok := s.data[f.Name]
		d.Chk.True(ok)
		res = append(res, v)
	}
	return
}

func (s Struct) Type() *Type {
	return s.t
}

func (s Struct) desc() StructDesc {
	return s.t.Desc.(StructDesc)
}

func (s Struct) MaybeGet(n string) (Value, bool) {
	_, ok := s.findField(n)
	if !ok {
		return nil, false
	}
	v, ok := s.data[n]
	return v, ok
}

func (s Struct) Get(n string) Value {
	_, ok := s.findField(n)
	d.Chk.True(ok, `Struct has no field "%s"`, n)
	v, ok := s.data[n]
	d.Chk.True(ok)
	return v
}

func (s Struct) Set(n string, v Value) Struct {
	f, ok := s.findField(n)
	d.Chk.True(ok, "Struct has no field %s", n)
	assertType(f.Type, v)
	data := make(structData, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	data[n] = v

	return newStructFromData(data, s.t)
}

func (s Struct) findField(n string) (Field, bool) {
	for _, f := range s.desc().Fields {
		if f.Name == n {
			return f, true
		}
	}
	return Field{}, false
}

func structBuilder(values []Value, t *Type) Value {
	desc := t.Desc.(StructDesc)
	data := structData{}

	for i, f := range desc.Fields {
		data[f.Name] = values[i]
	}

	return newStructFromData(data, t)
}

func structReader(s Struct, t *Type) []Value {
	d.Chk.Equal(t.Kind(), StructKind)
	values := []Value{}

	desc := t.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := s.data[f.Name]
		d.Chk.True(ok)
		values = append(values, v)
	}

	return values
}
