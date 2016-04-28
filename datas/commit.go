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
	//   value: Value
	//   parents: Set<Ref<Commit>>
	// }

	fieldTypes := []types.Field{
		types.Field{Name: ValueField, T: types.ValueType},
		types.Field{Name: ParentsField, T: nil},
	}
	commitType = types.MakeStructType(structName, fieldTypes)
	commitType.Desc.(types.StructDesc).Fields[1].T = types.MakeSetType(types.MakeRefType(commitType))
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
