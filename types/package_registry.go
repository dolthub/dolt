package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type enumBuilderFunc func(v uint32) Value
type enumReaderFunc func(v Value) uint32
type structBuilderFunc func() chan Value
type structReaderFunc func(v Value) chan Value
type valueBuilderFunc func(v Value) Value
type valueReaderFunc func(v Value) Value

type toNomsValueFunc func(v Value) Value

type enumFuncs struct {
	builder enumBuilderFunc
	reader  enumReaderFunc
}

type structFuncs struct {
	builder structBuilderFunc
	reader  structReaderFunc
}

type valueFuncs struct {
	builder valueBuilderFunc
	reader  valueReaderFunc
}

var (
	packages       map[ref.Ref]*Package        = map[ref.Ref]*Package{}
	toNomsValueMap map[ref.Ref]toNomsValueFunc = map[ref.Ref]toNomsValueFunc{}
	enumFuncMap    map[ref.Ref]enumFuncs       = map[ref.Ref]enumFuncs{}
	structFuncMap  map[ref.Ref]structFuncs     = map[ref.Ref]structFuncs{}
	valueFuncMap   map[ref.Ref]valueFuncs      = map[ref.Ref]valueFuncs{}
)

// LookupPackage looks for a Package by ref.Ref in the global cache of Noms type packages.
func LookupPackage(r ref.Ref) *Package {
	return packages[r]
}

// RegisterPackage puts p into the global cache of Noms type packages.
func RegisterPackage(p *Package) (r ref.Ref) {
	d.Chk.NotNil(p)
	r = p.Ref()
	packages[r] = p
	return
}

func readPackage(r ref.Ref, cs chunks.ChunkSource) *Package {
	p := ReadValue(r, cs).(Package)
	RegisterPackage(&p)
	return &p
}

func RegisterFromValFunction(t TypeRef, f toNomsValueFunc) {
	toNomsValueMap[t.Ref()] = f
}

func ToNomsValueFromTypeRef(t TypeRef, v Value) Value {
	if f, ok := toNomsValueMap[t.Ref()]; ok {
		return f(v)
	}
	return v
}

func RegisterStruct(t TypeRef, bf structBuilderFunc, rf structReaderFunc) {
	structFuncMap[t.Ref()] = structFuncs{bf, rf}
}

func structBuilderForTypeRef(typeRef, typeDef TypeRef) chan Value {
	if s, ok := structFuncMap[typeRef.Ref()]; ok {
		return s.builder()
	}
	return structBuilder(typeRef, typeDef)
}

func structReaderForTypeRef(v Value, typeRef, typeDef TypeRef) chan Value {
	if s, ok := structFuncMap[typeRef.Ref()]; ok {
		return s.reader(v)
	}
	return structReader(v.(Struct), typeRef, typeDef)
}

func RegisterEnum(t TypeRef, bf enumBuilderFunc, rf enumReaderFunc) {
	enumFuncMap[t.Ref()] = enumFuncs{bf, rf}
}

func enumFromTypeRef(v uint32, t TypeRef) Value {
	if s, ok := enumFuncMap[t.Ref()]; ok {
		return s.builder(v)
	}
	return newEnum(v, t)
}

func enumPrimitiveValueFromTypeRef(v Value, t TypeRef) uint32 {
	if s, ok := enumFuncMap[t.Ref()]; ok {
		return s.reader(v)
	}
	return v.(Enum).v
}

func RegisterValue(t TypeRef, bf valueBuilderFunc, rf valueReaderFunc) {
	valueFuncMap[t.Ref()] = valueFuncs{bf, rf}
}

func valueFromTypeRef(v Value, t TypeRef) Value {
	if s, ok := valueFuncMap[t.Ref()]; ok {
		return s.builder(v)
	}
	return v
}

func internalValueFromTypeRef(v Value, t TypeRef) Value {
	if s, ok := valueFuncMap[t.Ref()]; ok {
		return s.reader(v)
	}
	return v
}
