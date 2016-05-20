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

func NewStruct(name string, data structData) Struct {
	fields := make(TypeMap, len(data))
	newData := make(structData, len(data))
	for k, v := range data {
		fields[k] = v.Type()
		newData[k] = v
	}
	t := MakeStructType(name, fields)
	return newStructFromData(newData, t)
}

func NewStructWithType(t *Type, data structData) Struct {
	newData := make(structData, len(data))
	desc := t.Desc.(StructDesc)
	for name, t := range desc.Fields {
		v, ok := data[name]
		d.Chk.True(ok, "Missing required field %s", name)
		assertSubtype(t, v)
		newData[name] = v
	}
	return newStructFromData(newData, t)
}

// Value interface
func (s Struct) Equals(other Value) bool {
	return other != nil && s.t.Equals(other.Type()) && s.Ref() == other.Ref()
}

func (s Struct) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Struct) Ref() ref.Ref {
	return EnsureRef(s.ref, s)
}

func (s Struct) ChildValues() (res []Value) {
	res = append(res, s.t)
	for name := range s.desc().Fields {
		v, ok := s.data[name]
		d.Chk.True(ok)
		res = append(res, v)
	}
	return
}

func (s Struct) Chunks() (chunks []Ref) {
	chunks = append(chunks, s.t.Chunks()...)
	for name := range s.desc().Fields {
		v, ok := s.data[name]
		d.Chk.True(ok)
		chunks = append(chunks, v.Chunks()...)
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
	t, ok := s.findField(n)
	d.Chk.True(ok, "Struct has no field %s", n)
	assertSubtype(t, v)
	data := make(structData, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	data[n] = v

	return newStructFromData(data, s.t)
}

func (s Struct) findField(n string) (*Type, bool) {
	for name, typ := range s.desc().Fields {
		if name == n {
			return typ, true
		}
	}
	return nil, false
}

func structBuilder(values []Value, t *Type) Value {
	desc := t.Desc.(StructDesc)
	data := structData{}

	i := 0
	desc.IterFields(func(name string, t *Type) {
		data[name] = values[i]
		i++
	})

	return newStructFromData(data, t)
}

func structReader(s Struct, t *Type) []Value {
	d.Chk.Equal(t.Kind(), StructKind)
	values := []Value{}

	desc := t.Desc.(StructDesc)
	desc.IterFields(func(name string, t *Type) {
		v, ok := s.data[name]
		d.Chk.True(ok)
		values = append(values, v)
	})

	return values
}

// s1 & s2 must be of the same type. Returns the set of field names which have different values in the respective structs
func StructDiff(s1, s2 Struct) (changed []string) {
	d.Chk.True(s1.Type().Equals(s2.Type()))

	s1.desc().IterFields(func(name string, t *Type) {
		v1 := s1.data[name]
		v2 := s2.data[name]
		if !v1.Equals(v2) {
			changed = append(changed, name)
		}
	})

	return
}
