// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package ngql

import (
	"context"
	"fmt"

	"strings"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
)

type typeMap map[typeMapKey]graphql.Type

type typeMapKey struct {
	h             hash.Hash
	boxedIfScalar bool
}

func NewTypeMap() *typeMap {
	return &typeMap{}
}

// In terms of resolving a graph of data, there are three types of value:
// scalars, lists and maps. During resolution, we are converting some noms
// value to a graphql value. A getFieldFn will be invoked for a matching noms
// type. Its job is to retrieve the sub-value from the noms type which is
// mapped to a graphql map as a fieldname.
type getFieldFn func(v interface{}, fieldName string, ctx context.Context) types.Value

// When a field name is resolved, it may take key:value arguments. A
// getSubvaluesFn handles returning one or more *noms* values whose presence is
// indicated by the provided arguments.
type getSubvaluesFn func(v types.Value, args map[string]interface{}) (interface{}, error)

// GraphQL requires all memberTypes in a Union to be Structs, so when a noms
// union contains a scalar, we represent it in that context as a "boxed" value.
// E.g.
// Boolean! =>
// type BooleanValue {
//   scalarValue: Boolean!
// }
func scalarToValue(nomsType *types.Type, scalarType graphql.Type, tm *typeMap) graphql.Type {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sValue", getTypeName(nomsType)),
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
func NomsTypeToGraphQLType(nomsType *types.Type, boxedIfScalar bool, tm *typeMap) graphql.Type {
	key := typeMapKey{nomsType.Hash(), boxedIfScalar && isScalar(nomsType)}
	gqlType, ok := (*tm)[key]
	if ok {
		return gqlType
	}

	// The graphql package has built in support for recursive types using
	// FieldsThunk which allows the inner type to refer to an outer type by
	// lazily initializing the fields.
	switch nomsType.Kind() {
	case types.NumberKind:
		gqlType = graphql.Float
		if boxedIfScalar {
			gqlType = scalarToValue(nomsType, gqlType, tm)
		}

	case types.StringKind:
		gqlType = graphql.String
		if boxedIfScalar {
			gqlType = scalarToValue(nomsType, gqlType, tm)
		}

	case types.BoolKind:
		gqlType = graphql.Boolean
		if boxedIfScalar {
			gqlType = scalarToValue(nomsType, gqlType, tm)
		}

	case types.StructKind:
		gqlType = structToGQLObject(nomsType, tm)

	case types.ListKind, types.SetKind:
		gqlType = listAndSetToGraphQLObject(nomsType, tm)

	case types.MapKind:
		gqlType = mapToGraphQLObject(nomsType, tm)

	case types.RefKind:
		gqlType = refToGraphQLObject(nomsType, tm)

	case types.UnionKind:
		gqlType = unionToGQLUnion(nomsType, tm)

	case types.BlobKind, types.ValueKind, types.TypeKind:
		// TODO: https://github.com/attic-labs/noms/issues/3155
		gqlType = graphql.String

	case types.CycleKind:
		panic("not reached") // we should never attempt to create a schedule for any unresolved cycle

	default:
		panic("not reached")
	}

	(*tm)[key] = gqlType
	return gqlType
}

func isEmptyNomsUnion(nomsType *types.Type) bool {
	return nomsType.Kind() == types.UnionKind && len(nomsType.Desc.(types.CompoundDesc).ElemTypes) == 0
}

// Creates a union of structs type.
func unionToGQLUnion(nomsType *types.Type, tm *typeMap) *graphql.Union {
	nomsMemberTypes := nomsType.Desc.(types.CompoundDesc).ElemTypes
	memberTypes := make([]*graphql.Object, len(nomsMemberTypes))

	for i, nomsUnionType := range nomsMemberTypes {
		// Member types cannot be non-null and must be struct (graphl.Object)
		memberTypes[i] = NomsTypeToGraphQLType(nomsUnionType, true, tm).(*graphql.Object)
	}

	return graphql.NewUnion(graphql.UnionConfig{
		Name:  getTypeName(nomsType),
		Types: memberTypes,
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			tm := p.Context.Value(tmKey).(*typeMap)
			var nomsType *types.Type
			isScalar := false
			if v, ok := p.Value.(types.Value); ok {
				nomsType = v.Type()
			} else {
				switch p.Value.(type) {
				case float64:
					nomsType = types.NumberType
					isScalar = true
				case string:
					nomsType = types.StringType
					isScalar = true
				case bool:
					nomsType = types.BoolType
					isScalar = true
				}
			}
			key := typeMapKey{nomsType.Hash(), isScalar}
			memberType := (*tm)[key]
			// Member types cannot be non-null and must be struct (graphl.Object)
			return memberType.(*graphql.Object)
		},
	})
}

func structToGQLObject(nomsType *types.Type, tm *typeMap) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: getTypeName(nomsType),
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

			structDesc.IterFields(func(name string, nomsFieldType *types.Type) {
				fieldType := NomsTypeToGraphQLType(nomsFieldType, false, tm)

				fields[name] = &graphql.Field{
					Type: fieldType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						if field, ok := p.Source.(types.Struct).MaybeGet(p.Info.FieldName); ok {
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

var listArgs = graphql.FieldConfigArgument{
	atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
	countKey: &graphql.ArgumentConfig{Type: graphql.Int},
}

func getListElements(v types.Value, args map[string]interface{}) (interface{}, error) {
	l := v.(types.List)
	idx := 0
	count := int(l.Len())
	len := count

	if at, ok := args[atKey].(int); ok {
		idx = at
	}

	if c, ok := args[countKey].(int); ok {
		count = c
	}

	// Clamp ranges
	if count <= 0 || idx >= len {
		return ([]interface{})(nil), nil
	}
	if idx < 0 {
		idx = 0
	}
	if idx+count > len {
		count = len - idx
	}

	values := make([]interface{}, count)
	iter := l.IteratorAt(uint64(idx))
	for i := uint64(0); i < uint64(count); i++ {
		values[i] = MaybeGetScalar(iter.Next())
	}

	return values, nil
}

func getSetElements(v types.Value, args map[string]interface{}) (interface{}, error) {
	s := v.(types.Set)

	iter, nomsKey, nomsThrough, count, singleExactMatch, err := getCollectionArgs(s, args, iteratorFactory{
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

	if err != nil {
		return nil, err
	}
	if count == 0 {
		return ([]interface{})(nil), nil
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

	return values, nil
}

func getCollectionArgs(col types.Collection, args map[string]interface{}, factory iteratorFactory) (iter interface{}, nomsKey, nomsThrough types.Value, count uint64, singleExactMatch bool, err error) {
	typ := col.Type()
	length := col.Len()
	nomsKeyType := typ.Desc.(types.CompoundDesc).ElemTypes[0]

	if keys, ok := args[keysKey]; ok {
		slice := keys.([]interface{})
		nomsKeys := make(types.ValueSlice, len(slice))
		for i, v := range slice {
			var nomsValue types.Value
			nomsValue, err = marshal.Marshal(v)
			if err != nil {
				return
			}
			nomsKeys[i] = nomsValue
		}
		count = uint64(len(slice))
		iter = &mapIteratorForKeys{
			m:    col.(types.Map),
			keys: nomsKeys,
		}
		return
	}

	nomsThrough, err = getThroughArg(nomsKeyType, args)
	if err != nil {
		return
	}

	count, singleExactMatch = getCountArg(length, args)

	if key, ok := args[keyKey]; ok {
		nomsKey, err = marshal.Marshal(key)
		if err != nil {
			return
		}
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

func getMapElements(v types.Value, args map[string]interface{}, app mapAppender) (interface{}, error) {
	m := v.(types.Map)

	iter, nomsKey, nomsThrough, count, singleExactMatch, err := getCollectionArgs(m, args, iteratorFactory{
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

	if err != nil {
		return nil, err
	}
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

func getThroughArg(nomsKeyType *types.Type, args map[string]interface{}) (types.Value, error) {
	var nomsThrough types.Value
	if through, ok := args[throughKey]; ok {
		var err error
		nomsThrough, err = marshal.Marshal(through)
		if err != nil {
			return nil, err
		}
	}
	return nomsThrough, nil
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
func mapEntryToGraphQLObject(keyType, valueType graphql.Type, nomsKeyType, nomsValueType *types.Type, tm *typeMap) graphql.Type {
	return graphql.NewNonNull(graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%sEntry", getTypeName(nomsKeyType), getTypeName(nomsValueType)),
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

func getTypeName(nomsType *types.Type) string {
	switch nomsType.Kind() {
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
		return fmt.Sprintf("%sList", getTypeName(nomsValueType))

	case types.MapKind:
		nomsKeyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]
		if isEmptyNomsUnion(nomsKeyType) {
			d.Chk.True(isEmptyNomsUnion(nomsValueType))
			return "EmptyMap"
		}

		return fmt.Sprintf("%sTo%sMap", getTypeName(nomsKeyType), getTypeName(nomsValueType))

	case types.RefKind:
		return fmt.Sprintf("%sRef", getTypeName(nomsType.Desc.(types.CompoundDesc).ElemTypes[0]))

	case types.SetKind:
		nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
		if isEmptyNomsUnion(nomsValueType) {
			return "EmptySet"
		}

		return fmt.Sprintf("%sSet", getTypeName(nomsValueType))

	case types.StructKind:
		// GraphQL Name cannot start with a number.
		// GraphQL type names must be globally unique.
		return fmt.Sprintf("%s_%s", nomsType.Desc.(types.StructDesc).Name, nomsType.Hash().String()[:6])

	case types.TypeKind:
		// GraphQL Name cannot start with a number.
		// TODO: https://github.com/attic-labs/noms/issues/3155
		return fmt.Sprintf("Type_%s", nomsType.Hash().String()[:6])

	case types.UnionKind:
		unionMemberTypes := nomsType.Desc.(types.CompoundDesc).ElemTypes
		names := make([]string, len(unionMemberTypes))
		for i, unionMemberType := range unionMemberTypes {
			names[i] = getTypeName(unionMemberType)
		}
		return strings.Join(names, "Or")

	default:
		panic(fmt.Sprintf("%d: (getTypeName) not reached", nomsType.Kind()))
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

func listAndSetToGraphQLObject(nomsType *types.Type, tm *typeMap) *graphql.Object {
	nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
	var listType, valueType graphql.Type
	if !isEmptyNomsUnion(nomsValueType) {
		valueType = NomsTypeToGraphQLType(nomsValueType, false, tm)
		listType = graphql.NewNonNull(valueType)
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name: getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			fields := argsWithSize()

			if listType != nil {
				var args graphql.FieldConfigArgument
				var getSubvalues getSubvaluesFn

				switch nomsType.Kind() {
				case types.ListKind:
					args = listArgs
					getSubvalues = getListElements

				case types.SetKind:
					args = graphql.FieldConfigArgument{
						atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
						countKey: &graphql.ArgumentConfig{Type: graphql.Int},
					}
					if graphql.IsInputType(valueType) {
						// TODO: Should compute an graphql.InputObject from the noms type. graphql.InputObject is different from graphql.Object
						// See graphql.IsInputType vs graphql.IsOutputType
						args[keyKey] = &graphql.ArgumentConfig{Type: valueType}
						args[throughKey] = &graphql.ArgumentConfig{Type: valueType}
					}
					getSubvalues = getSetElements
				}
				valuesField := &graphql.Field{
					Type: graphql.NewList(listType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						return getSubvalues(c, p.Args)
					},
				}
				fields[valuesKey] = valuesField
				fields[elementsKey] = valuesField
			}

			return fields
		}),
	})
}

func mapToGraphQLObject(nomsType *types.Type, tm *typeMap) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			nomsKeyType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
			nomsValueType := nomsType.Desc.(types.CompoundDesc).ElemTypes[1]
			isEmptyMap := isEmptyNomsUnion(nomsKeyType) || isEmptyNomsUnion(nomsValueType)

			fields := argsWithSize()

			if !isEmptyMap {
				nullableKeyType := NomsTypeToGraphQLType(nomsKeyType, false, tm)
				keyType := graphql.NewNonNull(nullableKeyType)
				valueType := NomsTypeToGraphQLType(nomsValueType, false, tm)
				entryType := mapEntryToGraphQLObject(keyType, valueType, nomsKeyType, nomsValueType, tm)

				args := graphql.FieldConfigArgument{
					atKey:    &graphql.ArgumentConfig{Type: graphql.Int},
					countKey: &graphql.ArgumentConfig{Type: graphql.Int},
				}
				if graphql.IsInputType(keyType) {
					args[keyKey] = &graphql.ArgumentConfig{Type: nullableKeyType}
					args[keysKey] = &graphql.ArgumentConfig{Type: graphql.NewList(keyType)}
					args[throughKey] = &graphql.ArgumentConfig{Type: nullableKeyType}
				}

				entriesField := &graphql.Field{
					Type: graphql.NewList(entryType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						return getMapElements(c, p.Args, mapAppendEntry)
					},
				}
				fields[entriesKey] = entriesField
				fields[elementsKey] = entriesField

				fields[keysKey] = &graphql.Field{
					Type: graphql.NewList(keyType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						return getMapElements(c, p.Args, mapAppendKey)
					},
				}
				fields[valuesKey] = &graphql.Field{
					Type: graphql.NewList(valueType),
					Args: args,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						c := p.Source.(types.Collection)
						return getMapElements(c, p.Args, mapAppendValue)
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
func refToGraphQLObject(nomsType *types.Type, tm *typeMap) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: getTypeName(nomsType),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			nomsTargetType := nomsType.Desc.(types.CompoundDesc).ElemTypes[0]
			targetType := NomsTypeToGraphQLType(nomsTargetType, false, tm)

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
						return MaybeGetScalar(r.TargetValue(p.Context.Value(vrKey).(types.ValueReader))), nil
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
