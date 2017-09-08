// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"context"
	"errors"
	"fmt"

	"strings"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// TypeConverter provides functions to convert between Noms types and GraphQL
// types.
type TypeConverter struct {
	tm       TypeMap
	NameFunc NameFunc
}

// NewTypeConverter creates a new TypeConverter.
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{
		TypeMap{},
		DefaultNameFunc,
	}
}

// NameFunc defines how to compute the GraphQL name for a Noms type.
type NameFunc func(nomsType *types.Type, isInputType bool) string

func (tc *TypeConverter) getTypeName(nomsType *types.Type) string {
	return tc.NameFunc(nomsType, false)
}

func (tc *TypeConverter) getInputTypeName(nomsType *types.Type) string {
	return tc.NameFunc(nomsType, true)
}

// NomsTypeToGraphQLType creates a GraphQL type from a Noms type that knows how
// to resolve the Noms values.
func (tc *TypeConverter) NomsTypeToGraphQLType(nomsType *types.Type) graphql.Type {
	return tc.nomsTypeToGraphQLType(nomsType, false)
}

// NomsTypeToGraphQLInputType creates a GraphQL input type from a Noms type.
// Input types may not be unions or cyclic structs. If we encounter those
// this returns an error.
func (tc *TypeConverter) NomsTypeToGraphQLInputType(nomsType *types.Type) (graphql.Input, error) {
	return tc.nomsTypeToGraphQLInputType(nomsType)
}

// TypeMap is used as a cache in NomsTypeToGraphQLType and
// NomsTypeToGraphQLInputType.
type TypeMap map[typeMapKey]graphql.Type

type typeMapKey struct {
	name          string
	boxedIfScalar bool
}

// NewTypeMap creates a new map that is used as a cache in
// NomsTypeToGraphQLType and NomsTypeToGraphQLInputType.
func NewTypeMap() *TypeMap {
	return &TypeMap{}
}

// GraphQL has two type systems.
// - One for output types which is used with resolvers to produce an output set.
// - And another one for input types. Input types are used to verify that the
// JSON like data passes as arguments are of the right type.
// There is some overlap here. Scalars are the same and List can be used in
// both.
// The significant difference is graphql.Object (output) vs graphql.InputObject
// Input types cannot be unions and input object types cannot contain cycles.

type graphQLTypeMode uint8

const (
	inputMode graphQLTypeMode = iota
	outputMode
)

// In terms of resolving a graph of data, there are three types of value:
// scalars, lists and maps. During resolution, we are converting some noms
// value to a graphql value. A getFieldFn will be invoked for a matching noms
// type. Its job is to retrieve the sub-value from the noms type which is
// mapped to a graphql map as a fieldname.
type getFieldFn func(v interface{}, fieldName string, ctx context.Context) types.Value

// When a field name is resolved, it may take key:value arguments. A
// getSubvaluesFn handles returning one or more *noms* values whose presence is
// indicated by the provided arguments.
type getSubvaluesFn func(vrw types.ValueReadWriter, v types.Value, args map[string]interface{}) interface{}

// GraphQL requires all memberTypes in a Union to be Structs, so when a noms
// union contains a scalar, we represent it in that context as a "boxed" value.
// E.g.
// Boolean! =>
// type BooleanValue {
//   scalarValue: Boolean!
// }
func (tc *TypeConverter) scalarToValue(nomsType *types.Type, scalarType graphql.Type) graphql.Type {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sValue", tc.getTypeName(nomsType)),
		Fields: graphql.Fields{
			scalarValue: &graphql.Field{
				Type: graphql.NewNonNull(scalarType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return p.Source, nil // p.Source is already a go-native scalar type
				},
			},
		}})
}

func isScalar(nomsType *types.Type) bool {
	switch nomsType {
	case types.BoolType, types.NumberType, types.StringType:
		return true
	default:
		return false
	}
}

// NomsTypeToGraphQLType creates a GraphQL type from a Noms type that knows how
// to resolve the Noms values.
func NomsTypeToGraphQLType(nomsType *types.Type, boxedIfScalar bool, tm *TypeMap) graphql.Type {
	tc := TypeConverter{*tm, DefaultNameFunc}
	return tc.nomsTypeToGraphQLType(nomsType, boxedIfScalar)
}

func (tc *TypeConverter) nomsTypeToGraphQLType(nomsType *types.Type, boxedIfScalar bool) graphql.Type {
	name := tc.getTypeName(nomsType)
	key := typeMapKey{name, boxedIfScalar && isScalar(nomsType)}
	gqlType, ok := tc.tm[key]
	if ok {
		return gqlType
	}

	// The graphql package has built in support for recursive types using
	// FieldsThunk which allows the inner type to refer to an outer type by
	// lazily initializing the fields.
	switch nomsType.TargetKind() {
	case types.NumberKind:
		gqlType = graphql.Float
		if boxedIfScalar {
			gqlType = tc.scalarToValue(nomsType, gqlType)
		}

	case types.StringKind:
		gqlType = graphql.String
		if boxedIfScalar {
			gqlType = tc.scalarToValue(nomsType, gqlType)
		}

	case types.BoolKind:
		gqlType = graphql.Boolean
		if boxedIfScalar {
			gqlType = tc.scalarToValue(nomsType, gqlType)
		}

	case types.StructKind:
		gqlType = tc.structToGQLObject(nomsType)

	case types.ListKind, types.SetKind:
		gqlType = tc.listAndSetToGraphQLObject(nomsType)

	case types.MapKind:
		gqlType = tc.mapToGraphQLObject(nomsType)

	case types.RefKind:
		gqlType = tc.refToGraphQLObject(nomsType)

	case types.UnionKind:
		gqlType = tc.unionToGQLUnion(nomsType)

	case types.BlobKind, types.ValueKind, types.TypeKind:
		// TODO: https://github.com/attic-labs/noms/issues/3155
		gqlType = graphql.String

	case types.CycleKind:
		panic("not reached") // we should never attempt to create a schema for any unresolved cycle

	default:
		panic("not reached")
	}

	tc.tm[key] = gqlType
	return gqlType
}

// NomsTypeToGraphQLInputType creates a GraphQL input type from a Noms type.
// Input types may not be unions or cyclic structs. If we encounter those
// this returns an error.
func NomsTypeToGraphQLInputType(nomsType *types.Type, tm *TypeMap) (graphql.Input, error) {
	tc := TypeConverter{*tm, DefaultNameFunc}
	return tc.nomsTypeToGraphQLInputType(nomsType)
}

func (tc *TypeConverter) nomsTypeToGraphQLInputType(nomsType *types.Type) (graphql.Input, error) {
	// GraphQL input types do not support cycles.
	if types.HasStructCycles(nomsType) {
		return nil, errors.New("GraphQL input type cannot contain cycles")
	}

	name := tc.getInputTypeName(nomsType)
	key := typeMapKey{name, false}
	gqlType, ok := tc.tm[key]
	if ok {
		return gqlType, nil
	}

	var err error
	switch nomsType.TargetKind() {
	case types.NumberKind:
		gqlType = graphql.Float

	case types.StringKind:
		gqlType = graphql.String

	case types.BoolKind:
		gqlType = graphql.Boolean

	case types.StructKind:
		gqlType, err = tc.structToGQLInputObject(nomsType)

	case types.ListKind, types.SetKind:
		gqlType, err = tc.listAndSetToGraphQLInputObject(nomsType)

	case types.MapKind:
		gqlType, err = tc.mapToGraphQLInputObject(nomsType)

	case types.RefKind:
		gqlType = graphql.String

	case types.UnionKind:
		return nil, errors.New("GraphQL input type cannot contain unions")

	case types.BlobKind, types.ValueKind, types.TypeKind:
		// TODO: https://github.com/attic-labs/noms/issues/3155
		gqlType = graphql.String

	case types.CycleKind:
		panic("not reachable") // This is handled at the top of nomsTypeToGraphQLInputType

	default:
		panic("not reached")
	}

	if err != nil {
		return nil, err
	}

	tc.tm[key] = gqlType
	return gqlType, nil
}

func isEmptyNomsUnion(nomsType *types.Type) bool {
	return nomsType.TargetKind() == types.UnionKind && len(nomsType.Desc.(types.CompoundDesc).ElemTypes) == 0
}

// Creates a union of structs type.
func (tc *TypeConverter) unionToGQLUnion(nomsType *types.Type) *graphql.Union {
	nomsMemberTypes := nomsType.Desc.(types.CompoundDesc).ElemTypes
	memberTypes := make([]*graphql.Object, len(nomsMemberTypes))

	for i, nomsUnionType := range nomsMemberTypes {
		// Member types cannot be non-null and must be struct (graphl.Object)
		memberTypes[i] = tc.nomsTypeToGraphQLType(nomsUnionType, true).(*graphql.Object)
	}

	return graphql.NewUnion(graphql.UnionConfig{
		Name:  tc.getTypeName(nomsType),
		Types: memberTypes,
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			if v, ok := p.Value.(types.Value); ok {
				// We cannot just get the type of the value here. GraphQL requires
				// us to return one of the types in memberTypes.
				for i, t := range nomsMemberTypes {
					if types.IsValueSubtypeOf(v, t) {
						return memberTypes[i]
					}
				}
				return nil
			}

			var nomsType *types.Type
			switch p.Value.(type) {
			case float64:
				nomsType = types.NumberType
			case string:
				nomsType = types.StringType
			case bool:
				nomsType = types.BoolType
			}
			return tc.nomsTypeToGraphQLType(nomsType, true).(*graphql.Object)
		},
	})
}

func (tc *TypeConverter) structToGQLObject(nomsType *types.Type) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: tc.getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			structDesc := nomsType.Desc.(types.StructDesc)
			fields := graphql.Fields{
				"hash": &graphql.Field{
					Type: graphql.NewNonNull(graphql.String),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						return p.Source.(types.Struct).Hash().String(), nil
					},
				},
			}

			structDesc.IterFields(func(name string, nomsFieldType *types.Type, optional bool) {
				fieldType := tc.nomsTypeToGraphQLType(nomsFieldType, false)
				if !optional {
					fieldType = graphql.NewNonNull(fieldType)
				}

				fields[name] = &graphql.Field{
					Type: fieldType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						if field, ok := p.Source.(types.Struct).MaybeGet(name); ok {
							return MaybeGetScalar(field), nil
						}
						return nil, nil
					},
				}
			})

			return fields
		}),
	})
}

func (tc *TypeConverter) listAndSetToGraphQLInputObject(nomsType *types.Type) (graphql.Input, error) {
	nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
	elemType, err := tc.nomsTypeToGraphQLInputType(nomsValueType)
	if err != nil {
		return nil, err
	}
	return graphql.NewList(graphql.NewNonNull(elemType)), nil
}

func (tc *TypeConverter) mapToGraphQLInputObject(nomsType *types.Type) (graphql.Input, error) {
	nomsKeyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
	nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]

	keyType, err := tc.nomsTypeToGraphQLInputType(nomsKeyType)
	if err != nil {
		return nil, err
	}
	valueType, err := tc.nomsTypeToGraphQLInputType(nomsValueType)
	if err != nil {
		return nil, err
	}

	entryType := tc.mapEntryToGraphQLInputObject(keyType, valueType, nomsKeyType, nomsValueType)
	return graphql.NewList(entryType), nil
}

func (tc *TypeConverter) structToGQLInputObject(nomsType *types.Type) (graphql.Input, error) {
	var err error
	rv := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: tc.getInputTypeName(nomsType),
		Fields: graphql.InputObjectConfigFieldMapThunk(func() graphql.InputObjectConfigFieldMap {
			structDesc := nomsType.Desc.(types.StructDesc)
			fields := make(graphql.InputObjectConfigFieldMap, structDesc.Len())

			structDesc.IterFields(func(name string, nomsFieldType *types.Type, optional bool) {
				if err != nil {
					return
				}
				var fieldType graphql.Input
				fieldType, err = tc.nomsTypeToGraphQLInputType(nomsFieldType)
				if err != nil {
					return
				}
				if !optional {
					fieldType = graphql.NewNonNull(fieldType)
				}
				fields[name] = &graphql.InputObjectFieldConfig{
					Type: fieldType,
				}
			})

			return fields
		}),
	})
	if err != nil {
		return nil, err
	}
	return rv, nil
}

var listArgs = graphql.FieldConfigArgument{
	atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
	countKey: &graphql.ArgumentConfig{Type: graphql.Int},
}

func getListElements(vrw types.ValueReadWriter, v types.Value, args map[string]interface{}) interface{} {
	l := v.(types.Collection)
	idx := 0
	count := int(l.Len())
	end := count

	if at, ok := args[atKey].(int); ok {
		idx = at
	}

	if c, ok := args[countKey].(int); ok {
		count = c
	}

	// Clamp ranges
	if count <= 0 || idx >= end {
		return ([]interface{})(nil)
	}
	if idx < 0 {
		idx = 0
	}
	if idx+count > end {
		count = end - idx
	}

	values := make([]interface{}, count)

	cols, offset := types.LoadLeafNodes([]types.Collection{l}, uint64(idx), uint64(idx+count))

	// Iterate the collections we got, skipping the first offset elements and bailing out
	// once we've filled values with count elements.
	elementsSeen := uint64(0)
	maybeAddElement := func(v types.Value) {
		if elementsSeen >= offset && elementsSeen-offset < uint64(count) {
			values[elementsSeen-offset] = MaybeGetScalar(v)
		}
		elementsSeen++
	}
	// TODO: Use a cursor so we do not have to instantiate all values. @arv has a
	// change in the works that only creates Values as needed.
	for _, c := range cols {
		v := c.(types.Value)
		v.WalkValues(maybeAddElement)
		if elementsSeen-offset >= uint64(count) {
			break
		}
	}

	return values
}

func getSetElements(vrw types.ValueReadWriter, v types.Value, args map[string]interface{}) interface{} {
	s := v.(types.Set)

	iter, nomsKey, nomsThrough, count, singleExactMatch := getCollectionArgs(vrw, s, args, iteratorFactory{
		IteratorFrom: func(from types.Value) interface{} {
			return s.IteratorFrom(from)
		},
		IteratorAt: func(at uint64) interface{} {
			return s.IteratorAt(at)
		},
		First: func() interface{} {
			return &setFirstIterator{s: s}
		},
	})

	if count == 0 {
		return ([]interface{})(nil)
	}

	setIter := iter.(types.SetIterator)
	values := make([]interface{}, 0, count)
	for i := uint64(0); i < count; i++ {
		v := setIter.Next()
		if v == nil {
			break
		}
		if singleExactMatch {
			if nomsKey.Equals(v) {
				values = append(values, MaybeGetScalar(v))
			}
			break
		}

		if nomsThrough != nil {
			if !nomsThrough.Less(v) {
				values = append(values, MaybeGetScalar(v))
			} else {
				break
			}
		} else {
			values = append(values, MaybeGetScalar(v))
		}
	}

	return values
}

func getCollectionArgs(vrw types.ValueReadWriter, col types.Collection, args map[string]interface{}, factory iteratorFactory) (iter interface{}, nomsKey, nomsThrough types.Value, count uint64, singleExactMatch bool) {
	typ := types.TypeOf(col)
	length := col.Len()
	nomsKeyType := typ.Desc.(types.CompoundDesc).ElemTypes[0]

	if keys, ok := args[keysKey]; ok {
		slice := keys.([]interface{})
		nomsKeys := make(types.ValueSlice, len(slice))
		for i, v := range slice {
			var nomsValue types.Value
			nomsValue = InputToNomsValue(vrw, v, nomsKeyType)
			nomsKeys[i] = nomsValue
		}
		count = uint64(len(slice))
		iter = &mapIteratorForKeys{
			m:    col.(types.Map),
			keys: nomsKeys,
		}
		return
	}

	nomsThrough = getThroughArg(vrw, nomsKeyType, args)

	count, singleExactMatch = getCountArg(length, args)

	if key, ok := args[keyKey]; ok {
		nomsKey = InputToNomsValue(vrw, key, nomsKeyType)
		iter = factory.IteratorFrom(nomsKey)
	} else if at, ok := args[atKey]; ok {
		idx := at.(int)
		if idx < 0 {
			idx = 0
		} else if uint64(idx) > length {
			count = 0
			return
		}
		iter = factory.IteratorAt(uint64(idx))
	} else if count == 1 && !singleExactMatch {
		// no key, no at, no through, but a count:1
		iter = factory.First()
	} else {
		iter = factory.IteratorAt(0)
	}

	return
}

type mapAppender func(slice []interface{}, k, v types.Value) []interface{}

func getMapElements(vrw types.ValueReadWriter, v types.Value, args map[string]interface{}, app mapAppender) (interface{}, error) {
	m := v.(types.Map)

	iter, nomsKey, nomsThrough, count, singleExactMatch := getCollectionArgs(vrw, m, args, iteratorFactory{
		IteratorFrom: func(from types.Value) interface{} {
			return m.IteratorFrom(from)
		},
		IteratorAt: func(at uint64) interface{} {
			return m.IteratorAt(at)
		},
		First: func() interface{} {
			return &mapFirstIterator{m: m}
		},
	})

	if count == 0 {
		return ([]interface{})(nil), nil
	}

	mapIter := iter.(types.MapIterator)
	values := make([]interface{}, 0, count)
	for i := uint64(0); i < count; i++ {
		k, v := mapIter.Next()
		if k == nil {
			break
		}

		if singleExactMatch {
			if nomsKey.Equals(k) {
				values = app(values, k, v)
			}
			break
		}

		if nomsThrough != nil {
			if !nomsThrough.Less(k) {
				values = app(values, k, v)
			} else {
				break
			}
		} else {
			values = app(values, k, v)
		}
	}

	return values, nil
}

func getCountArg(count uint64, args map[string]interface{}) (c uint64, singleExactMatch bool) {
	if c, ok := args[countKey]; ok {
		c := c.(int)
		if c <= 0 {
			return 0, false
		}
		return uint64(c), false
	}
	// If we have key and no count/through we use count 1
	_, hasKey := args[keyKey]
	_, hasThrough := args[throughKey]
	if hasKey && !hasThrough {
		return uint64(1), true
	}

	return count, false
}

func getThroughArg(vrw types.ValueReadWriter, nomsKeyType *types.Type, args map[string]interface{}) types.Value {
	if through, ok := args[throughKey]; ok {
		return InputToNomsValue(vrw, through, nomsKeyType)
	}
	return nil
}

type iteratorFactory struct {
	IteratorFrom func(from types.Value) interface{}
	IteratorAt   func(at uint64) interface{}
	First        func() interface{}
}

type mapEntry struct {
	key, value types.Value
}

// Map data must be returned as a list of key-value pairs. Each unique keyType:valueType is
// represented as a graphql
//
// type <KeyTypeName><ValueTypeName>Entry {
//	 key: <KeyType>!
//	 value: <ValueType>!
// }
func (tc *TypeConverter) mapEntryToGraphQLObject(keyType, valueType graphql.Type, nomsKeyType, nomsValueType *types.Type) graphql.Type {
	return graphql.NewNonNull(graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%sEntry", tc.getTypeName(nomsKeyType), tc.getTypeName(nomsValueType)),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				keyKey: &graphql.Field{
					Type: keyType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						entry := p.Source.(mapEntry)
						return MaybeGetScalar(entry.key), nil
					},
				},
				valueKey: &graphql.Field{
					Type: valueType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						entry := p.Source.(mapEntry)
						return MaybeGetScalar(entry.value), nil
					},
				},
			}
		}),
	}))
}

func (tc *TypeConverter) mapEntryToGraphQLInputObject(keyType, valueType graphql.Input, nomsKeyType, nomsValueType *types.Type) graphql.Input {
	return graphql.NewNonNull(graphql.NewInputObject(graphql.InputObjectConfig{
		Name: fmt.Sprintf("%s%sEntryInput", tc.getInputTypeName(nomsKeyType), tc.getInputTypeName(nomsValueType)),
		Fields: graphql.InputObjectConfigFieldMapThunk(func() graphql.InputObjectConfigFieldMap {
			return graphql.InputObjectConfigFieldMap{
				keyKey: &graphql.InputObjectFieldConfig{
					Type: graphql.NewNonNull(keyType),
				},
				valueKey: &graphql.InputObjectFieldConfig{
					Type: graphql.NewNonNull(valueType),
				},
			}
		}),
	}))
}

// DefaultNameFunc returns the GraphQL type name for a Noms type.
func DefaultNameFunc(nomsType *types.Type, isInputType bool) string {
	if isInputType {
		return GetInputTypeName(nomsType)
	}
	return GetTypeName(nomsType)
}

// GetTypeName provides a unique type name that is used by GraphQL.
func GetTypeName(nomsType *types.Type) string {
	return getTypeName(nomsType, "")
}

// GetInputTypeName returns a type name that is unique and useful for GraphQL
// input types.
func GetInputTypeName(nomsType *types.Type) string {
	return getTypeName(nomsType, "Input")
}

func getTypeName(nomsType *types.Type, suffix string) string {
	switch nomsType.TargetKind() {
	case types.BoolKind:
		return "Boolean"

	case types.NumberKind:
		return "Number"

	case types.StringKind:
		return "String"

	case types.BlobKind:
		return "Blob"

	case types.ValueKind:
		return "Value"

	case types.ListKind:
		nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		if isEmptyNomsUnion(nomsValueType) {
			return "EmptyList"
		}
		return fmt.Sprintf("%sList%s", GetTypeName(nomsValueType), suffix)

	case types.MapKind:
		nomsKeyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]
		if isEmptyNomsUnion(nomsKeyType) {
			d.Chk.True(isEmptyNomsUnion(nomsValueType))
			return "EmptyMap"
		}

		return fmt.Sprintf("%sTo%sMap%s", GetTypeName(nomsKeyType), GetTypeName(nomsValueType), suffix)

	case types.RefKind:
		return fmt.Sprintf("%sRef%s", GetTypeName(nomsType.Desc.(types.CompoundDesc).ElemTypes[0]), suffix)

	case types.SetKind:
		nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		if isEmptyNomsUnion(nomsValueType) {
			return "EmptySet"
		}

		return fmt.Sprintf("%sSet%s", GetTypeName(nomsValueType), suffix)

	case types.StructKind:
		// GraphQL Name cannot start with a number.
		// GraphQL type names must be globally unique.
		return fmt.Sprintf("%s%s_%s", nomsType.Desc.(types.StructDesc).Name, suffix, nomsType.Hash().String()[:6])

	case types.TypeKind:
		// GraphQL Name cannot start with a number.
		// TODO: https://github.com/attic-labs/noms/issues/3155
		return fmt.Sprintf("Type%s_%s", suffix, nomsType.Hash().String()[:6])

	case types.UnionKind:
		unionMemberTypes := nomsType.Desc.(types.CompoundDesc).ElemTypes
		names := make([]string, len(unionMemberTypes))
		for i, unionMemberType := range unionMemberTypes {
			names[i] = GetTypeName(unionMemberType)
		}
		return strings.Join(names, "Or") + suffix

	case types.CycleKind:
		return "Cycle"

	default:
		panic(fmt.Sprintf("(GetTypeName) not reached: %s", nomsType.Describe()))
	}
}

func argsWithSize() graphql.Fields {
	return graphql.Fields{
		sizeKey: &graphql.Field{
			Type: graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				c := p.Source.(types.Collection)
				return MaybeGetScalar(types.Number(c.Len())), nil
			},
		},
	}
}

func (tc *TypeConverter) listAndSetToGraphQLObject(nomsType *types.Type) *graphql.Object {
	nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
	var listType, valueType graphql.Type
	var keyInputType graphql.Input
	var keyInputError error
	if !isEmptyNomsUnion(nomsValueType) {
		valueType = tc.nomsTypeToGraphQLType(nomsValueType, false)
		keyInputType, keyInputError = tc.nomsTypeToGraphQLInputType(nomsValueType)
		listType = graphql.NewNonNull(valueType)
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name: tc.getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			fields := argsWithSize()

			if listType != nil {
				var args graphql.FieldConfigArgument
				var getSubvalues getSubvaluesFn

				switch nomsType.TargetKind() {
				case types.ListKind:
					args = listArgs
					getSubvalues = getListElements

				case types.SetKind:
					args = graphql.FieldConfigArgument{
						atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
						countKey: &graphql.ArgumentConfig{Type: graphql.Int},
					}
					if keyInputError == nil {
						args[keyKey] = &graphql.ArgumentConfig{Type: keyInputType}
						args[throughKey] = &graphql.ArgumentConfig{Type: keyInputType}
					}
					getSubvalues = getSetElements
				}
				valuesField := &graphql.Field{
					Type: graphql.NewList(listType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						vrw := p.Context.Value(vrwKey).(types.ValueReadWriter)
						return getSubvalues(vrw, c, p.Args), nil
					},
				}
				fields[valuesKey] = valuesField
				fields[elementsKey] = valuesField
			}

			return fields
		}),
	})
}

func (tc *TypeConverter) mapToGraphQLObject(nomsType *types.Type) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: tc.getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			nomsKeyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
			nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]
			isEmptyMap := isEmptyNomsUnion(nomsKeyType) || isEmptyNomsUnion(nomsValueType)

			fields := argsWithSize()

			if !isEmptyMap {
				keyType := tc.nomsTypeToGraphQLType(nomsKeyType, false)
				keyInputType, keyInputError := tc.nomsTypeToGraphQLInputType(nomsKeyType)
				valueType := tc.nomsTypeToGraphQLType(nomsValueType, false)
				entryType := tc.mapEntryToGraphQLObject(graphql.NewNonNull(keyType), valueType, nomsKeyType, nomsValueType)

				args := graphql.FieldConfigArgument{
					atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
					countKey: &graphql.ArgumentConfig{Type: graphql.Int},
				}
				if keyInputError == nil {
					args[keyKey] = &graphql.ArgumentConfig{Type: keyInputType}
					args[keysKey] = &graphql.ArgumentConfig{Type: graphql.NewList(graphql.NewNonNull(keyInputType))}
					args[throughKey] = &graphql.ArgumentConfig{Type: keyInputType}
				}

				entriesField := &graphql.Field{
					Type: graphql.NewList(entryType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						vrw := p.Context.Value(vrwKey).(types.ValueReadWriter)
						return getMapElements(vrw, c, p.Args, mapAppendEntry)
					},
				}
				fields[entriesKey] = entriesField
				fields[elementsKey] = entriesField

				fields[keysKey] = &graphql.Field{
					Type: graphql.NewList(keyType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						vrw := p.Context.Value(vrwKey).(types.ValueReadWriter)
						return getMapElements(vrw, c, p.Args, mapAppendKey)
					},
				}
				fields[valuesKey] = &graphql.Field{
					Type: graphql.NewList(valueType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						vrw := p.Context.Value(vrwKey).(types.ValueReadWriter)
						return getMapElements(vrw, c, p.Args, mapAppendValue)
					},
				}
			}

			return fields
		}),
	})
}

func mapAppendKey(slice []interface{}, k, v types.Value) []interface{} {
	return append(slice, MaybeGetScalar(k))
}

func mapAppendValue(slice []interface{}, k, v types.Value) []interface{} {
	return append(slice, MaybeGetScalar(v))
}

func mapAppendEntry(slice []interface{}, k, v types.Value) []interface{} {
	return append(slice, mapEntry{k, v})
}

// Refs are represented as structs:
//
// type <ValueTypeName>Entry {
//	 targetHash: String!
//	 targetValue: <ValueType>!
// }
func (tc *TypeConverter) refToGraphQLObject(nomsType *types.Type) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: tc.getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			nomsTargetType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
			targetType := tc.nomsTypeToGraphQLType(nomsTargetType, false)

			return graphql.Fields{
				targetHashKey: &graphql.Field{
					Type: graphql.NewNonNull(graphql.String),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						r := p.Source.(types.Ref)
						return MaybeGetScalar(types.String(r.TargetHash().String())), nil
					},
				},

				targetValueKey: &graphql.Field{
					Type: targetType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						r := p.Source.(types.Ref)
						return MaybeGetScalar(r.TargetValue(p.Context.Value(vrwKey).(types.ValueReader))), nil
					},
				},
			}
		}),
	})
}

func MaybeGetScalar(v types.Value) interface{} {
	switch v.(type) {
	case types.Bool:
		return bool(v.(types.Bool))
	case types.Number:
		return float64(v.(types.Number))
	case types.String:
		return string(v.(types.String))
	case *types.Type, types.Blob:
		// TODO: https://github.com/attic-labs/noms/issues/3155
		return v.Hash()
	}

	return v
}

// InputToNomsValue converts a GraphQL input value (as used in arguments and
// variables) to a Noms value.
func InputToNomsValue(vrw types.ValueReadWriter, arg interface{}, nomsType *types.Type) types.Value {
	switch nomsType.TargetKind() {
	case types.BoolKind:
		return types.Bool(arg.(bool))
	case types.NumberKind:
		if i, ok := arg.(int); ok {
			return types.Number(i)
		}
		return types.Number(arg.(float64))
	case types.StringKind:
		return types.String(arg.(string))
	case types.ListKind, types.SetKind:
		elemType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		sl := arg.([]interface{})
		vs := make(types.ValueSlice, len(sl))
		for i, v := range sl {
			vs[i] = InputToNomsValue(vrw, v, elemType)
		}
		if nomsType.TargetKind() == types.ListKind {
			return types.NewList(vrw, vs...)
		}
		return types.NewSet(vrw, vs...)
	case types.MapKind:
		// Maps are passed as [{key: K, value: V}, ...]
		keyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		valType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]
		sl := arg.([]interface{})
		kvs := make(types.ValueSlice, 2*len(sl))
		for i, v := range sl {
			v := v.(map[string]interface{})
			kvs[2*i] = InputToNomsValue(vrw, v["key"], keyType)
			kvs[2*i+1] = InputToNomsValue(vrw, v["value"], valType)
		}
		return types.NewMap(vrw, kvs...)
	case types.StructKind:
		desc := nomsType.Desc.(types.StructDesc)
		data := make(types.StructData, desc.Len())
		m := arg.(map[string]interface{})
		desc.IterFields(func(name string, t *types.Type, optional bool) {
			if m[name] != nil || !optional {
				data[name] = InputToNomsValue(vrw, m[name], t)
			}
		})
		return types.NewStruct(desc.Name, data)
	}
	panic("not yet implemented")
}

type mapIteratorForKeys struct {
	m    types.Map
	keys types.ValueSlice
	idx  int
}

func (it *mapIteratorForKeys) Next() (k, v types.Value) {
	if it.idx >= len(it.keys) {
		return
	}
	k = it.keys[it.idx]
	v = it.m.Get(k)
	it.idx++
	return
}

type setFirstIterator struct {
	s types.Set
}

func (it *setFirstIterator) Next() types.Value {
	return it.s.First()
}

func (it *setFirstIterator) SkipTo(v types.Value) types.Value {
	panic("not implemented")
}

type mapFirstIterator struct {
	m types.Map
}

func (it *mapFirstIterator) Next() (types.Value, types.Value) {
	return it.m.First()
}
