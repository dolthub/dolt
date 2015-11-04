package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type structData map[string]Value

type Struct struct {
	data       structData
	t          TypeRef
	typeDef    TypeRef
	unionIndex uint32
	unionValue Value
	ref        *ref.Ref
}

func newStructFromData(data structData, unionIndex uint32, unionValue Value, typeRef, typeDef TypeRef) Struct {
	d.Chk.Equal(typeRef.Kind(), UnresolvedKind)
	d.Chk.True(typeRef.HasPackageRef())
	d.Chk.True(typeRef.HasOrdinal())
	d.Chk.Equal(typeDef.Kind(), StructKind)
	return Struct{data, typeRef, typeDef, unionIndex, unionValue, &ref.Ref{}}
}

func NewStruct(typeRef, typeDef TypeRef, data structData) Struct {
	newData := make(structData)
	unionIndex := uint32(0)
	var unionValue Value

	desc := typeDef.Desc.(StructDesc)
	for _, f := range desc.Fields {
		v, ok := data[f.Name]
		if ok {
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
	return newStructFromData(newData, unionIndex, unionValue, typeRef, typeDef)
}

func (s Struct) Equals(other Value) bool {
	return other != nil && s.t.Equals(other.TypeRef()) && s.Ref() == other.Ref()
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

func (s Struct) TypeRef() TypeRef {
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

func structBuilder(typeRef, typeDef TypeRef) chan Value {
	c := make(chan Value)

	go func() {
		desc := typeDef.Desc.(StructDesc)
		data := structData{}
		unionIndex := uint32(0)
		var unionValue Value

		for _, f := range desc.Fields {
			if f.Optional {
				b := bool((<-c).(Bool))
				if b {
					data[f.Name] = <-c
				}
			} else {
				data[f.Name] = <-c
			}
		}
		if len(desc.Union) > 0 {
			unionIndex = uint32((<-c).(UInt32))
			unionValue = <-c
		}

		c <- newStructFromData(data, unionIndex, unionValue, typeRef, typeDef)
	}()

	return c
}
