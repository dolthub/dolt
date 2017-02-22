package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/language/ast"
)

var someScalarType = graphql.NewScalar(graphql.ScalarConfig{
	Name: "SomeScalar",
	Serialize: func(value interface{}) interface{} {
		return nil
	},
	ParseValue: func(value interface{}) interface{} {
		return nil
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		return nil
	},
})
var someObjectType = graphql.NewObject(graphql.ObjectConfig{
	Name: "SomeObject",
	Fields: graphql.Fields{
		"f": &graphql.Field{
			Type: graphql.String,
		},
	},
})
var objectWithIsTypeOf = graphql.NewObject(graphql.ObjectConfig{
	Name: "ObjectWithIsTypeOf",
	IsTypeOf: func(p graphql.IsTypeOfParams) bool {
		return true
	},
	Fields: graphql.Fields{
		"f": &graphql.Field{
			Type: graphql.String,
		},
	},
})
var someUnionType = graphql.NewUnion(graphql.UnionConfig{
	Name: "SomeUnion",
	ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
		return nil
	},
	Types: []*graphql.Object{
		someObjectType,
	},
})
var someInterfaceType = graphql.NewInterface(graphql.InterfaceConfig{
	Name: "SomeInterface",
	ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
		return nil
	},
	Fields: graphql.Fields{
		"f": &graphql.Field{
			Type: graphql.String,
		},
	},
})
var someEnumType = graphql.NewEnum(graphql.EnumConfig{
	Name: "SomeEnum",
	Values: graphql.EnumValueConfigMap{
		"ONLY": &graphql.EnumValueConfig{},
	},
})
var someInputObject = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "SomeInputObject",
	Fields: graphql.InputObjectConfigFieldMap{
		"f": &graphql.InputObjectFieldConfig{
			Type:         graphql.String,
			DefaultValue: "Hello",
		},
	},
})

func withModifiers(ttypes []graphql.Type) []graphql.Type {
	res := ttypes
	for _, ttype := range ttypes {
		res = append(res, graphql.NewList(ttype))
	}
	for _, ttype := range ttypes {
		res = append(res, graphql.NewNonNull(ttype))
	}
	for _, ttype := range ttypes {
		res = append(res, graphql.NewNonNull(graphql.NewList(ttype)))
	}
	return res
}

var outputTypes = withModifiers([]graphql.Type{
	graphql.String,
	someScalarType,
	someEnumType,
	someObjectType,
	someUnionType,
	someInterfaceType,
})
var inputTypes = withModifiers([]graphql.Type{
	graphql.String,
	someScalarType,
	someEnumType,
	someInputObject,
})

func schemaWithFieldType(ttype graphql.Output) (graphql.Schema, error) {
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: ttype,
				},
			},
		}),
		Types: []graphql.Type{ttype},
	})
}
func schemaWithInputObject(ttype graphql.Input) (graphql.Schema, error) {
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: graphql.String,
					Args: graphql.FieldConfigArgument{
						"args": &graphql.ArgumentConfig{
							Type: ttype,
						},
					},
				},
			},
		}),
	})
}
func schemaWithObjectFieldOfType(fieldType graphql.Input) (graphql.Schema, error) {

	badObjectType := graphql.NewObject(graphql.ObjectConfig{
		Name: "BadObject",
		Fields: graphql.Fields{
			"badField": &graphql.Field{
				Type: fieldType,
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: badObjectType,
				},
			},
		}),
	})
}
func schemaWithObjectImplementingType(implementedType *graphql.Interface) (graphql.Schema, error) {

	badObjectType := graphql.NewObject(graphql.ObjectConfig{
		Name:       "BadObject",
		Interfaces: []*graphql.Interface{implementedType},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: badObjectType,
				},
			},
		}),
		Types: []graphql.Type{badObjectType},
	})
}
func schemaWithUnionOfType(ttype *graphql.Object) (graphql.Schema, error) {

	badObjectType := graphql.NewUnion(graphql.UnionConfig{
		Name: "BadUnion",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Types: []*graphql.Object{ttype},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: badObjectType,
				},
			},
		}),
	})
}
func schemaWithInterfaceFieldOfType(ttype graphql.Type) (graphql.Schema, error) {

	badInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "BadInterface",
		Fields: graphql.Fields{
			"badField": &graphql.Field{
				Type: ttype,
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: badInterfaceType,
				},
			},
		}),
	})
}
func schemaWithArgOfType(ttype graphql.Type) (graphql.Schema, error) {

	badObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "BadObject",
		Fields: graphql.Fields{
			"badField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"badArg": &graphql.ArgumentConfig{
						Type: ttype,
					},
				},
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: badObject,
				},
			},
		}),
	})
}
func schemaWithInputFieldOfType(ttype graphql.Type) (graphql.Schema, error) {

	badInputObject := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "BadInputObject",
		Fields: graphql.InputObjectConfigFieldMap{
			"badField": &graphql.InputObjectFieldConfig{
				Type: ttype,
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"f": &graphql.Field{
					Type: graphql.String,
					Args: graphql.FieldConfigArgument{
						"badArg": &graphql.ArgumentConfig{
							Type: badInputObject,
						},
					},
				},
			},
		}),
	})
}

func TestTypeSystem_SchemaMustHaveObjectRootTypes_AcceptsASchemaWhoseQueryTypeIsAnObjectType(t *testing.T) {
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: someObjectType,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_SchemaMustHaveObjectRootTypes_AcceptsASchemaWhoseQueryAndMutationTypesAreObjectType(t *testing.T) {
	mutationObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"edit": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    someObjectType,
		Mutation: mutationObject,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_SchemaMustHaveObjectRootTypes_AcceptsASchemaWhoseQueryAndSubscriptionTypesAreObjectType(t *testing.T) {
	subscriptionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Subscription",
		Fields: graphql.Fields{
			"subscribe": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    someObjectType,
		Mutation: subscriptionType,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_SchemaMustHaveObjectRootTypes_RejectsASchemaWithoutAQueryType(t *testing.T) {
	_, err := graphql.NewSchema(graphql.SchemaConfig{})
	expectedError := "Schema query must be Object Type but got: nil."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_SchemaMustContainUniquelyNamedTypes_RejectsASchemaWhichRedefinesABuiltInType(t *testing.T) {

	fakeString := graphql.NewScalar(graphql.ScalarConfig{
		Name: "String",
		Serialize: func(value interface{}) interface{} {
			return nil
		},
	})
	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"normal": &graphql.Field{
				Type: graphql.String,
			},
			"fake": &graphql.Field{
				Type: fakeString,
			},
		},
	})
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: queryType,
	})
	expectedError := `Schema must contain unique named types but contains multiple types named "String".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_SchemaMustContainUniquelyNamedTypes_RejectsASchemaWhichDefinesAnObjectTypeTwice(t *testing.T) {

	a := graphql.NewObject(graphql.ObjectConfig{
		Name: "SameName",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	b := graphql.NewObject(graphql.ObjectConfig{
		Name: "SameName",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"a": &graphql.Field{
				Type: a,
			},
			"b": &graphql.Field{
				Type: b,
			},
		},
	})
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: queryType,
	})
	expectedError := `Schema must contain unique named types but contains multiple types named "SameName".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_SchemaMustContainUniquelyNamedTypes_RejectsASchemaWhichHaveSameNamedObjectsImplementingAnInterface(t *testing.T) {

	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	FirstBadObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "BadObject",
		Interfaces: []*graphql.Interface{
			anotherInterface,
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	SecondBadObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "BadObject",
		Interfaces: []*graphql.Interface{
			anotherInterface,
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"iface": &graphql.Field{
				Type: anotherInterface,
			},
		},
	})
	_, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: queryType,
		Types: []graphql.Type{FirstBadObject, SecondBadObject},
	})
	expectedError := `Schema must contain unique named types but contains multiple types named "BadObject".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustHaveFields_AcceptsAnObjectTypeWithFieldsObject(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_ObjectsMustHaveFields_RejectsAnObjectTypeWithMissingFields(t *testing.T) {
	badObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
	})
	_, err := schemaWithFieldType(badObject)
	expectedError := `SomeObject fields must be an object with field names as keys or a function which return such an object.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ObjectsMustHaveFields_RejectsAnObjectTypeWithIncorrectlyNamedFields(t *testing.T) {
	badObject := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: graphql.Fields{
			"bad-name-with-dashes": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(badObject)
	expectedError := `Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but "bad-name-with-dashes" does not.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ObjectsMustHaveFields_RejectsAnObjectTypeWithEmptyFields(t *testing.T) {
	badObject := graphql.NewObject(graphql.ObjectConfig{
		Name:   "SomeObject",
		Fields: graphql.Fields{},
	})
	_, err := schemaWithFieldType(badObject)
	expectedError := `SomeObject fields must be an object with field names as keys or a function which return such an object.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_FieldsArgsMustBeProperlyNamed_AcceptsFieldArgsWithValidNames(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: graphql.Fields{
			"goodField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"goodArgs": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_FieldsArgsMustBeProperlyNamed_RejectsFieldArgWithInvalidNames(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: graphql.Fields{
			"badField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"bad-name-with-dashes": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	}))
	expectedError := `Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but "bad-name-with-dashes" does not.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_FieldsArgsMustBeObjects_AcceptsAnObjectTypeWithFieldArgs(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: graphql.Fields{
			"goodField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"goodArgs": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_ObjectInterfacesMustBeArray_AcceptsAnObjectTypeWithArrayInterfaces(t *testing.T) {
	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{anotherInterfaceType}
		}),
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_ObjectInterfacesMustBeArray_AcceptsAnObjectTypeWithInterfacesAsFunctionReturningAnArray(t *testing.T) {
	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name:       "SomeObject",
		Interfaces: []*graphql.Interface{anotherInterfaceType},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_UnionTypesMustBeArray_AcceptsAUnionTypeWithArrayTypes(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name: "SomeUnion",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Types: []*graphql.Object{
			someObjectType,
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_UnionTypesMustBeArray_RejectsAUnionTypeWithoutTypes(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name: "SomeUnion",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
	}))
	expectedError := "Must provide Array of types for Union SomeUnion."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_UnionTypesMustBeArray_RejectsAUnionTypeWithEmptyTypes(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name: "SomeUnion",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Types: []*graphql.Object{},
	}))
	expectedError := "Must provide Array of types for Union SomeUnion."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_InputObjectsMustHaveFields_AcceptsAnInputObjectTypeWithFields(t *testing.T) {
	_, err := schemaWithInputObject(graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SomeInputObject",
		Fields: graphql.InputObjectConfigFieldMap{
			"f": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_InputObjectsMustHaveFields_AcceptsAnInputObjectTypeWithAFieldFunction(t *testing.T) {
	_, err := schemaWithInputObject(graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SomeInputObject",
		Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
			return graphql.InputObjectConfigFieldMap{
				"f": &graphql.InputObjectFieldConfig{
					Type: graphql.String,
				},
			}
		}),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_InputObjectsMustHaveFields_RejectsAnInputObjectTypeWithMissingFields(t *testing.T) {
	_, err := schemaWithInputObject(graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SomeInputObject",
	}))
	expectedError := "SomeInputObject fields must be an object with field names as keys or a function which return such an object."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_InputObjectsMustHaveFields_RejectsAnInputObjectTypeWithEmptyFields(t *testing.T) {
	_, err := schemaWithInputObject(graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "SomeInputObject",
		Fields: graphql.InputObjectConfigFieldMap{},
	}))
	expectedError := "SomeInputObject fields must be an object with field names as keys or a function which return such an object."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectTypesMustBeAssertable_AcceptsAnObjectTypeWithAnIsTypeOfFunction(t *testing.T) {
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name: "AnotherObject",
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_InterfaceTypesMustBeResolvable_AcceptsAnInterfaceTypeDefiningResolveType(t *testing.T) {

	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name:       "SomeObject",
		Interfaces: []*graphql.Interface{anotherInterfaceType},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_InterfaceTypesMustBeResolvable_AcceptsAnInterfaceWithImplementingTypeDefiningIsTypeOf(t *testing.T) {

	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name:       "SomeObject",
		Interfaces: []*graphql.Interface{anotherInterfaceType},
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_InterfaceTypesMustBeResolvable_AcceptsAnInterfaceTypeDefiningResolveTypeWithImplementingTypeDefiningIsTypeOf(t *testing.T) {

	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(graphql.NewObject(graphql.ObjectConfig{
		Name:       "SomeObject",
		Interfaces: []*graphql.Interface{anotherInterfaceType},
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeSystem_UnionTypesMustBeResolvable_AcceptsAUnionTypeDefiningResolveType(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name:  "SomeUnion",
		Types: []*graphql.Object{someObjectType},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_UnionTypesMustBeResolvable_AcceptsAUnionOfObjectTypesDefiningIsTypeOf(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name:  "SomeUnion",
		Types: []*graphql.Object{objectWithIsTypeOf},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_UnionTypesMustBeResolvable_AcceptsAUnionTypeDefiningResolveTypeOfObjectTypesDefiningIsTypeOf(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name:  "SomeUnion",
		Types: []*graphql.Object{objectWithIsTypeOf},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_UnionTypesMustBeResolvable_RejectsAUnionTypeNotDefiningResolveTypeOfObjectTypesNotDefiningIsTypeOf(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewUnion(graphql.UnionConfig{
		Name:  "SomeUnion",
		Types: []*graphql.Object{someObjectType},
	}))
	expectedError := `Union Type SomeUnion does not provide a "resolveType" function and ` +
		`possible Type SomeObject does not provide a "isTypeOf" function. ` +
		`There is no way to resolve this possible type during execution.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ScalarTypesMustBeSerializable_AcceptsAScalarTypeDefiningSerialize(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewScalar(graphql.ScalarConfig{
		Name: "SomeScalar",
		Serialize: func(value interface{}) interface{} {
			return nil
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_ScalarTypesMustBeSerializable_RejectsAScalarTypeNotDefiningSerialize(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewScalar(graphql.ScalarConfig{
		Name: "SomeScalar",
	}))
	expectedError := `SomeScalar must provide "serialize" function. If this custom Scalar ` +
		`is also used as an input type, ensure "parseValue" and "parseLiteral" ` +
		`functions are also provided.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ScalarTypesMustBeSerializable_AcceptsAScalarTypeDefiningParseValueAndParseLiteral(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewScalar(graphql.ScalarConfig{
		Name: "SomeScalar",
		Serialize: func(value interface{}) interface{} {
			return nil
		},
		ParseValue: func(value interface{}) interface{} {
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			return nil
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_ScalarTypesMustBeSerializable_RejectsAScalarTypeDefiningParseValueButNotParseLiteral(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewScalar(graphql.ScalarConfig{
		Name: "SomeScalar",
		Serialize: func(value interface{}) interface{} {
			return nil
		},
		ParseValue: func(value interface{}) interface{} {
			return nil
		},
	}))
	expectedError := `SomeScalar must provide both "parseValue" and "parseLiteral" functions.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ScalarTypesMustBeSerializable_RejectsAScalarTypeDefiningParseLiteralButNotParseValue(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewScalar(graphql.ScalarConfig{
		Name: "SomeScalar",
		Serialize: func(value interface{}) interface{} {
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			return nil
		},
	}))
	expectedError := `SomeScalar must provide both "parseValue" and "parseLiteral" functions.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_EnumTypesMustBeWellDefined_AcceptsAWellDefinedEnumTypeWithEmptyValueDefinition(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewEnum(graphql.EnumConfig{
		Name: "SomeEnum",
		Values: graphql.EnumValueConfigMap{
			"FOO": &graphql.EnumValueConfig{},
			"BAR": &graphql.EnumValueConfig{},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_EnumTypesMustBeWellDefined_AcceptsAWellDefinedEnumTypeWithInternalValueDefinition(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewEnum(graphql.EnumConfig{
		Name: "SomeEnum",
		Values: graphql.EnumValueConfigMap{
			"FOO": &graphql.EnumValueConfig{
				Value: 10,
			},
			"BAR": &graphql.EnumValueConfig{
				Value: 20,
			},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestTypeSystem_EnumTypesMustBeWellDefined_RejectsAnEnumTypeWithoutValues(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewEnum(graphql.EnumConfig{
		Name: "SomeEnum",
	}))
	expectedError := `SomeEnum values must be an object with value names as keys.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_EnumTypesMustBeWellDefined_RejectsAnEnumTypeWithEmptyValues(t *testing.T) {

	_, err := schemaWithFieldType(graphql.NewEnum(graphql.EnumConfig{
		Name:   "SomeEnum",
		Values: graphql.EnumValueConfigMap{},
	}))
	expectedError := `SomeEnum values must be an object with value names as keys.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectFieldsMustHaveOutputTypes_AcceptAnOutputTypeAsAnObjectFieldType(t *testing.T) {
	for _, ttype := range outputTypes {
		_, err := schemaWithObjectFieldOfType(ttype)
		if err != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, err, ttype)
		}
	}
}
func TestTypeSystem_ObjectFieldsMustHaveOutputTypes_RejectsAnEmptyObjectFieldType(t *testing.T) {
	_, err := schemaWithObjectFieldOfType(nil)
	expectedError := `BadObject.badField field type must be Output Type but got: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsCanOnlyImplementInterfaces_AcceptsAnObjectImplementingAnInterface(t *testing.T) {
	anotherInterfaceType := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithObjectImplementingType(anotherInterfaceType)
	if err != nil {
		t.Fatalf(`unexpected error: %v"`, err)
	}
}
func TestTypeSystem_ObjectsCanOnlyImplementInterfaces_RejectsAnObjectImplementingANonInterfaceType(t *testing.T) {
	_, err := schemaWithObjectImplementingType(nil)
	expectedError := `BadObject may only implement Interface types, it cannot implement: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_UnionsMustRepresentObjectTypes_AcceptsAUnionOfAnObjectType(t *testing.T) {
	_, err := schemaWithUnionOfType(someObjectType)
	if err != nil {
		t.Fatalf(`unexpected error: %v"`, err)
	}
}
func TestTypeSystem_UnionsMustRepresentObjectTypes_RejectsAUnionOfNonObjectTypes(t *testing.T) {
	_, err := schemaWithUnionOfType(nil)
	expectedError := `BadUnion may only contain Object types, it cannot contain: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_InterfaceFieldsMustHaveOutputTypes_AcceptsAnOutputTypeAsAnInterfaceFieldType(t *testing.T) {
	for _, ttype := range outputTypes {
		_, err := schemaWithInterfaceFieldOfType(ttype)
		if err != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, err, ttype)
		}
	}
}
func TestTypeSystem_InterfaceFieldsMustHaveOutputTypes_RejectsAnEmptyInterfaceFieldType(t *testing.T) {
	_, err := schemaWithInterfaceFieldOfType(nil)
	expectedError := `BadInterface.badField field type must be Output Type but got: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_FieldArgumentsMustHaveInputTypes_AcceptsAnInputTypeAsFieldArgType(t *testing.T) {
	for _, ttype := range inputTypes {
		_, err := schemaWithArgOfType(ttype)
		if err != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, err, ttype)
		}
	}
}
func TestTypeSystem_FieldArgumentsMustHaveInputTypes_RejectsAnEmptyFieldArgType(t *testing.T) {
	_, err := schemaWithArgOfType(nil)
	expectedError := `BadObject.badField(badArg:) argument type must be Input Type but got: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_InputObjectFieldsMustHaveInputTypes_AcceptsAnInputTypeAsInputFieldType(t *testing.T) {
	for _, ttype := range inputTypes {
		_, err := schemaWithInputFieldOfType(ttype)
		if err != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, err, ttype)
		}
	}
}
func TestTypeSystem_InputObjectFieldsMustHaveInputTypes_RejectsAnEmptyInputFieldType(t *testing.T) {
	_, err := schemaWithInputFieldOfType(nil)
	expectedError := `BadInputObject.badField field type must be Input Type but got: <nil>.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ListMustAcceptGraphQLTypes_AcceptsAnTypeAsItemTypeOfList(t *testing.T) {
	testTypes := withModifiers([]graphql.Type{
		graphql.String,
		someScalarType,
		someEnumType,
		someObjectType,
		someUnionType,
		someInterfaceType,
	})
	for _, ttype := range testTypes {
		result := graphql.NewList(ttype)
		if result.Error() != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, result.Error(), ttype)
		}
	}
}
func TestTypeSystem_ListMustAcceptGraphQLTypes_RejectsANilTypeAsItemTypeOfList(t *testing.T) {
	result := graphql.NewList(nil)
	expectedError := `Can only create List of a Type but got: <nil>.`
	if result.Error() == nil || result.Error().Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, result.Error())
	}
}

func TestTypeSystem_NonNullMustAcceptGraphQLTypes_AcceptsAnTypeAsNullableTypeOfNonNull(t *testing.T) {
	nullableTypes := []graphql.Type{
		graphql.String,
		someScalarType,
		someObjectType,
		someUnionType,
		someInterfaceType,
		someEnumType,
		someInputObject,
		graphql.NewList(graphql.String),
		graphql.NewList(graphql.NewNonNull(graphql.String)),
	}
	for _, ttype := range nullableTypes {
		result := graphql.NewNonNull(ttype)
		if result.Error() != nil {
			t.Fatalf(`unexpected error: %v for type "%v"`, result.Error(), ttype)
		}
	}
}
func TestTypeSystem_NonNullMustAcceptGraphQLTypes_RejectsNilAsNonNullableType(t *testing.T) {
	result := graphql.NewNonNull(nil)
	expectedError := `Can only create NonNull of a Nullable Type but got: <nil>.`
	if result.Error() == nil || result.Error().Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, result.Error())
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWhichImplementsAnInterface(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWhichImplementsAnInterfaceAlongWithMoreFields(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
			"anotherfield": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWhichImpementsAnInterfaceFieldAlongWithAdditionalOptionalArguments(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
					"anotherInput": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWhichImplementsAnInterfaceFieldAlongWithAdditionalRequiredArguments(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
					"anotherInput": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
				},
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `AnotherObject.field(anotherInput:) is of required type "String!" but is not also provided by the interface AnotherInterface.field.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectMissingAnInterfaceField(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"anotherfield": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `"AnotherInterface" expects field "field" but "AnotherObject" does not provide it.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithAnIncorrectlyTypedInterfaceField(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: someScalarType,
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `AnotherInterface.field expects type "String" but AnotherObject.field provides type "SomeScalar".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithADifferentlyTypeInterfaceField(t *testing.T) {

	typeA := graphql.NewObject(graphql.ObjectConfig{
		Name: "A",
		Fields: graphql.Fields{
			"foo": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	typeB := graphql.NewObject(graphql.ObjectConfig{
		Name: "B",
		Fields: graphql.Fields{
			"foo": &graphql.Field{
				Type: graphql.String,
			},
		},
	})

	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: typeA,
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: typeB,
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `AnotherInterface.field expects type "A" but AnotherObject.field provides type "B".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWithASubtypedInterfaceField_Interface(t *testing.T) {
	var anotherInterface *graphql.Interface
	anotherInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			return graphql.Fields{
				"field": &graphql.Field{
					Type: anotherInterface,
				},
			}
		}),
	})
	var anotherObject *graphql.Object
	anotherObject = graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			return graphql.Fields{
				"field": &graphql.Field{
					Type: anotherObject,
				},
			}
		}),
	})
	_, err := schemaWithFieldType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWithASubtypedInterfaceField_Union(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: someUnionType,
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: someObjectType,
			},
		},
	})
	_, err := schemaWithFieldType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectMissingAnInterfaceArgument(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `AnotherInterface.field expects argument "input" but AnotherObject.field does not provide it.`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithAnIncorrectlyTypedInterfaceArgument(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: someScalarType,
					},
				},
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	expectedError := `AnotherInterface.field(input:) expects type "String" but AnotherObject.field(input:) provides type "SomeScalar".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWithAnEquivalentlyModifiedInterfaceField(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
			},
		},
	})
	_, err := schemaWithObjectFieldOfType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}
func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithANonListInterfaceFieldListType(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewList(graphql.String),
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(anotherObject)
	expectedError := `AnotherInterface.field expects type "[String]" but AnotherObject.field provides type "String".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithAListInterfaceFieldNonListType(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewList(graphql.String),
			},
		},
	})
	_, err := schemaWithFieldType(anotherObject)
	expectedError := `AnotherInterface.field expects type "String" but AnotherObject.field provides type "[String]".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_AcceptsAnObjectWithSubsetNonNullInterfaceFieldType(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
	})
	_, err := schemaWithFieldType(anotherObject)
	if err != nil {
		t.Fatalf(`unexpected error: %v for type "%v"`, err, anotherObject)
	}
}

func TestTypeSystem_ObjectsMustAdhereToInterfaceTheyImplement_RejectsAnObjectWithASupersetNullableInterfaceFieldType(t *testing.T) {
	anotherInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "AnotherInterface",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return nil
		},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
	})
	anotherObject := graphql.NewObject(graphql.ObjectConfig{
		Name:       "AnotherObject",
		Interfaces: []*graphql.Interface{anotherInterface},
		Fields: graphql.Fields{
			"field": &graphql.Field{
				Type: graphql.String,
			},
		},
	})
	_, err := schemaWithFieldType(anotherObject)
	expectedError := `AnotherInterface.field expects type "String!" but AnotherObject.field provides type "String".`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error: %v, got %v", expectedError, err)
	}
}
