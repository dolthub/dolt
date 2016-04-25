package datas

import (
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var __typeForCommit *types.Type
var __typeDef *types.Type

const (
	ParentsField = "parents"
	ValueField   = "value"
)

func init() {
	structName := "Commit"

	fieldTypes := []types.Field{
		types.Field{Name: ValueField, T: types.MakePrimitiveType(types.ValueKind)},
		types.Field{Name: ParentsField, T: types.MakeSetType(types.MakeRefType(types.MakeType(ref.Ref{}, 0)))},
	}

	typeDef := types.MakeStructType(structName, fieldTypes, []types.Field{})
	pkg := types.NewPackage([]*types.Type{typeDef}, []ref.Ref{})
	__typeDef = pkg.Types()[0]
	pkgRef := types.RegisterPackage(&pkg)
	__typeForCommit = types.MakeType(pkgRef, 0)
}

func NewCommit() types.Struct {
	initialFields := map[string]types.Value{
		ValueField:   types.NewString(""),
		ParentsField: NewSetOfRefOfCommit(),
	}

	return types.NewStruct(__typeForCommit, __typeDef, initialFields)
}

func typeForMapOfStringToRefOfCommit() *types.Type {
	return types.MakeMapType(types.StringType, types.MakeRefType(__typeForCommit))
}

func NewMapOfStringToRefOfCommit() types.Map {
	return types.NewTypedMap(typeForMapOfStringToRefOfCommit())
}

func typeForSetOfRefOfCommit() *types.Type {
	return types.MakeSetType(types.MakeRefType(__typeForCommit))
}

func NewSetOfRefOfCommit() types.Set {
	return types.NewTypedSet(typeForSetOfRefOfCommit())
}
