package datas

import "github.com/attic-labs/noms/types"

var commitType *types.Type

const (
	ParentsField = "parents"
	ValueField   = "value"
)

func init() {
	structName := "Commit"

	// struct Commit {
	//   parents: Set<Ref<Commit>>
	//   value: Value
	// }

	fieldTypes := types.TypeMap{
		ParentsField: nil,
		ValueField:   types.ValueType,
	}
	commitType = types.MakeStructType(structName, fieldTypes)
	commitType.Desc.(types.StructDesc).Fields[ParentsField] = types.MakeSetType(types.MakeRefType(commitType))
}

func NewCommit() types.Struct {
	initialFields := map[string]types.Value{
		ValueField:   types.NewString(""),
		ParentsField: NewSetOfRefOfCommit(),
	}

	return types.NewStruct(commitType, initialFields)
}

func typeForMapOfStringToRefOfCommit() *types.Type {
	return types.MakeMapType(types.StringType, types.MakeRefType(commitType))
}

func NewMapOfStringToRefOfCommit() types.Map {
	return types.NewTypedMap(typeForMapOfStringToRefOfCommit())
}

func typeForSetOfRefOfCommit() *types.Type {
	return types.MakeSetType(types.MakeRefType(commitType))
}

func NewSetOfRefOfCommit() types.Set {
	return types.NewTypedSet(typeForSetOfRefOfCommit())
}
