package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type enumBuilderFunc func(v uint32) Value
type enumReaderFunc func(v Value) uint32
type refBuilderFunc func(target ref.Ref) Value
type structBuilderFunc func() chan Value
type structReaderFunc func(v Value) chan Value
type valueBuilderFunc func(v Value) Value
type valueReaderFunc func(v Value) Value

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
	packages map[ref.Ref]*Package = map[ref.Ref]*Package{}

	enumFuncMap   map[ref.Ref]enumFuncs      = map[ref.Ref]enumFuncs{}
	refFuncMap    map[ref.Ref]refBuilderFunc = map[ref.Ref]refBuilderFunc{}
	structFuncMap map[ref.Ref]structFuncs    = map[ref.Ref]structFuncs{}
	valueFuncMap  map[ref.Ref]valueFuncs     = map[ref.Ref]valueFuncs{}
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

func RegisterRef(t TypeRef, bf refBuilderFunc) {
	refFuncMap[t.Ref()] = bf
}

func refFromTypeRef(target ref.Ref, t TypeRef) Value {
	if f, ok := refFuncMap[t.Ref()]; ok {
		return f(target)
	}
	return NewRef(target)
}
