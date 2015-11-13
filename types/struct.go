package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type structData map[string]Value

type Struct struct {
	data       structData
	t          Type
	typeDef    Type
	unionIndex uint32
	unionValue Value
	ref        *ref.Ref
}

func newStructFromData(data structData, unionIndex uint32, unionValue Value, typ, typeDef Type) Struct {
	d.Chk.Equal(typ.Kind(), UnresolvedKind)
	d.Chk.True(typ.HasPackageRef())
	d.Chk.True(typ.HasOrdinal())
	d.Chk.Equal(typeDef.Kind(), StructKind)
	return Struct{data, typ, typeDef, unionIndex, unionValue, &ref.Ref{}}
}

func NewStruct(typ, typeDef Type, data structData) Struct {
	newData := make(structData)
	unionIndex := uint32(0)
	var unionValue Value

	desc := typeDef.Desc.(StructDesc)
	for _, f := range desc.Fields {
		if v, ok := data[f.Name]; ok {
			newData[f.Name] = v
		} else {
			d.Chk.True(f.Optional, "Missing required field %s", f.Name)
		}
	}
	for i, f := range desc.Union {
		v, ok := data[f.Name]
		if ok {
			unionIndex = uint32(i)
			unionValue = v
			break
		}
	}
	return newStructFromData(newData, unionIndex, unionValue, typ, typeDef)
}

func (s Struct) Equals(other Value) bool {
	return other != nil && s.t.Equals(other.Type()) && s.Ref() == other.Ref()
}

func (s Struct) Ref() ref.Ref {
	return EnsureRef(s.ref, s)
}

func (s Struct) Chunks() (chunks []ref.Ref) {
	chunks = append(chunks, s.t.Chunks()...)
	for _, f := range s.desc().Fields {
		if v, ok := s.data[f.Name]; ok {
			chunks = append(chunks, v.Chunks()...)
		} else {
			d.Chk.True(f.Optional)
		}
	}

	if s.hasUnion() {
		chunks = append(chunks, s.unionValue.Chunks()...)
	}

	return
}

func (s Struct) ChildValues() (res []Value) {
	res = append(res, s.t)
	for _, f := range s.desc().Fields {
		if v, ok := s.data[f.Name]; ok {
			res = append(res, v)
		} else {
			d.Chk.True(f.Optional)
		}
	}
	if s.hasUnion() {
		res = append(res, s.unionValue)
	}
	return
}

func (s Struct) Type() Type {
	return s.t
}

func (s Struct) desc() StructDesc {
	return s.typeDef.Desc.(StructDesc)
}

func (s Struct) hasUnion() bool {
	return len(s.desc().Union) > 0
}

func (s Struct) MaybeGet(n string) (Value, bool) {
	_, idx, ok := s.findField(n)
	if !ok {
		return nil, false
	}
	if idx == -1 {
		v, ok := s.data[n]
		return v, ok
	}
	if s.unionIndex != uint32(idx) {
		return nil, false
	}
	return s.unionValue, true
}

func (s Struct) Get(n string) Value {
	_, idx, ok := s.findField(n)
	d.Chk.True(ok, `Struct has no field "%s"`, n)
	if idx == -1 {
		v, ok := s.data[n]
		d.Chk.True(ok)
		return v
	}
	d.Chk.Equal(s.unionIndex, uint32(idx), `Union field "%s" is not set`, n)
	return s.unionValue
}

func (s Struct) Set(n string, v Value) Struct {
	f, idx, ok := s.findField(n)
	d.Chk.True(ok, "Struct has no field %s", n)
	assertType(f.T, v)
	data := make(structData, len(s.data))
	unionIndex := s.unionIndex
	unionValue := s.unionValue
	for k, v := range s.data {
		data[k] = v
	}

	if idx == -1 {
		data[n] = v
	} else {
		unionIndex = uint32(idx)
		unionValue = v
	}

	return newStructFromData(data, unionIndex, unionValue, s.t, s.typeDef)
}

func (s Struct) UnionIndex() uint32 {
	return s.unionIndex
}

func (s Struct) UnionValue() Value {
	return s.unionValue
}

func (s Struct) findField(n string) (Field, int32, bool) {
	for _, f := range s.desc().Fields {
		if f.Name == n {
			return f, -1, true
		}
	}
	for i, f := range s.desc().Union {
		if f.Name == n {
			return f, int32(i), true
		}
	}
	return Field{}, -1, false
}

func structBuilder(values []Value, typ, typeDef Type) Value {
	i := 0
	desc := typeDef.Desc.(StructDesc)
	data := structData{}
	unionIndex := uint32(0)
	var unionValue Value

	for _, f := range desc.Fields {
		if f.Optional {
			b := bool(values[i].(Bool))
			i++
			if b {
				data[f.Name] = values[i]
				i++
			}
		} else {
			data[f.Name] = values[i]
			i++
		}
	}
	if len(desc.Union) > 0 {
		unionIndex = uint32(values[i].(UInt32))
		i++
		unionValue = values[i]
		i++
	}

	return newStructFromData(data, unionIndex, unionValue, typ, typeDef)
}

func structReader(s Struct, typ, typeDef Type) []Value {
	values := []Value{}

	desc := typeDef.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := s.data[f.Name]
		if f.Optional {
			values = append(values, Bool(ok))
			if ok {
				values = append(values, v)
			}
		} else {
			d.Chk.True(ok)
			values = append(values, v)
		}
	}
	if len(desc.Union) > 0 {
		values = append(values, UInt32(s.unionIndex), s.unionValue)
	}

	return values
}
