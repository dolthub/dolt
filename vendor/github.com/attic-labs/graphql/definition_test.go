package graphql_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/testutil"
)

var blogImage = graphql.NewObject(graphql.ObjectConfig{
	Name: "Image",
	Fields: graphql.Fields{
		"url": &graphql.Field{
			Type: graphql.String,
		},
		"width": &graphql.Field{
			Type: graphql.Int,
		},
		"height": &graphql.Field{
			Type: graphql.Int,
		},
	},
})
var blogAuthor = graphql.NewObject(graphql.ObjectConfig{
	Name: "Author",
	Fields: graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.String,
		},
		"name": &graphql.Field{
			Type: graphql.String,
		},
		"pic": &graphql.Field{
			Type: blogImage,
			Args: graphql.FieldConfigArgument{
				"width": &graphql.ArgumentConfig{
					Type: graphql.Int,
				},
				"height": &graphql.ArgumentConfig{
					Type: graphql.Int,
				},
			},
		},
		"recentArticle": &graphql.Field{},
	},
})
var blogArticle = graphql.NewObject(graphql.ObjectConfig{
	Name: "Article",
	Fields: graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.String,
		},
		"isPublished": &graphql.Field{
			Type: graphql.Boolean,
		},
		"author": &graphql.Field{
			Type: blogAuthor,
		},
		"title": &graphql.Field{
			Type: graphql.String,
		},
		"body": &graphql.Field{
			Type: graphql.String,
		},
	},
})

var blogQuery = graphql.NewObject(graphql.ObjectConfig{
	Name: "Query",
	Fields: graphql.Fields{
		"article": &graphql.Field{
			Type: blogArticle,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
		},
		"feed": &graphql.Field{
			Type: graphql.NewList(blogArticle),
		},
	},
})

var blogMutation = graphql.NewObject(graphql.ObjectConfig{
	Name: "Mutation",
	Fields: graphql.Fields{
		"writeArticle": &graphql.Field{
			Type: blogArticle,
			Args: graphql.FieldConfigArgument{
				"title": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
		},
	},
})

var blogSubscription = graphql.NewObject(graphql.ObjectConfig{
	Name: "Subscription",
	Fields: graphql.Fields{
		"articleSubscribe": &graphql.Field{
			Type: blogArticle,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
		},
	},
})

var objectType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Object",
	IsTypeOf: func(p graphql.IsTypeOfParams) bool {
		return true
	},
})
var interfaceType = graphql.NewInterface(graphql.InterfaceConfig{
	Name: "Interface",
})
var unionType = graphql.NewUnion(graphql.UnionConfig{
	Name: "Union",
	Types: []*graphql.Object{
		objectType,
	},
})
var enumType = graphql.NewEnum(graphql.EnumConfig{
	Name: "Enum",
	Values: graphql.EnumValueConfigMap{
		"foo": &graphql.EnumValueConfig{},
	},
})
var inputObjectType = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "InputObject",
})

func init() {
	blogAuthor.AddFieldConfig("recentArticle", &graphql.Field{
		Type: blogArticle,
	})
}

func TestTypeSystem_DefinitionExample_DefinesAQueryOnlySchema(t *testing.T) {
	blogSchema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: blogQuery,
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}

	if blogSchema.QueryType() != blogQuery {
		t.Fatalf("expected blogSchema.GetQueryType() == blogQuery")
	}

	articleField, _ := blogQuery.Fields()["article"]
	if articleField == nil {
		t.Fatalf("articleField is nil")
	}
	articleFieldType := articleField.Type
	if articleFieldType != blogArticle {
		t.Fatalf("articleFieldType expected to equal blogArticle, got: %v", articleField.Type)
	}
	if articleFieldType.Name() != "Article" {
		t.Fatalf("articleFieldType.Name expected to equal `Article`, got: %v", articleField.Type.Name())
	}
	if articleField.Name != "article" {
		t.Fatalf("articleField.Name expected to equal `article`, got: %v", articleField.Name)
	}
	articleFieldTypeObject, ok := articleFieldType.(*graphql.Object)
	if !ok {
		t.Fatalf("expected articleFieldType to be graphql.Object`, got: %v", articleField)
	}

	// TODO: expose a Object.GetField(key string), instead of this ghetto way of accessing a field map?
	titleField := articleFieldTypeObject.Fields()["title"]
	if titleField == nil {
		t.Fatalf("titleField is nil")
	}
	if titleField.Name != "title" {
		t.Fatalf("titleField.Name expected to equal title, got: %v", titleField.Name)
	}
	if titleField.Type != graphql.String {
		t.Fatalf("titleField.Type expected to equal graphql.String, got: %v", titleField.Type)
	}
	if titleField.Type.Name() != "String" {
		t.Fatalf("titleField.Type.GetName() expected to equal `String`, got: %v", titleField.Type.Name())
	}

	authorField := articleFieldTypeObject.Fields()["author"]
	if authorField == nil {
		t.Fatalf("authorField is nil")
	}
	authorFieldObject, ok := authorField.Type.(*graphql.Object)
	if !ok {
		t.Fatalf("expected authorField.Type to be Object`, got: %v", authorField)
	}

	recentArticleField := authorFieldObject.Fields()["recentArticle"]
	if recentArticleField == nil {
		t.Fatalf("recentArticleField is nil")
	}
	if recentArticleField.Type != blogArticle {
		t.Fatalf("recentArticleField.Type expected to equal blogArticle, got: %v", recentArticleField.Type)
	}

	feedField := blogQuery.Fields()["feed"]
	feedFieldList, ok := feedField.Type.(*graphql.List)
	if !ok {
		t.Fatalf("expected feedFieldList to be List`, got: %v", authorField)
	}
	if feedFieldList.OfType != blogArticle {
		t.Fatalf("feedFieldList.OfType expected to equal blogArticle, got: %v", feedFieldList.OfType)
	}
	if feedField.Name != "feed" {
		t.Fatalf("feedField.Name expected to equal `feed`, got: %v", feedField.Name)
	}
}

func TestTypeSystem_DefinitionExample_DefinesAMutationScheme(t *testing.T) {
	blogSchema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    blogQuery,
		Mutation: blogMutation,
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}

	if blogSchema.MutationType() != blogMutation {
		t.Fatalf("expected blogSchema.GetMutationType() == blogMutation")
	}

	writeMutation, _ := blogMutation.Fields()["writeArticle"]
	if writeMutation == nil {
		t.Fatalf("writeMutation is nil")
	}
	writeMutationType := writeMutation.Type
	if writeMutationType != blogArticle {
		t.Fatalf("writeMutationType expected to equal blogArticle, got: %v", writeMutationType)
	}
	if writeMutationType.Name() != "Article" {
		t.Fatalf("writeMutationType.Name expected to equal `Article`, got: %v", writeMutationType.Name())
	}
	if writeMutation.Name != "writeArticle" {
		t.Fatalf("writeMutation.Name expected to equal `writeArticle`, got: %v", writeMutation.Name)
	}
}

func TestTypeSystem_DefinitionExample_DefinesASubscriptionScheme(t *testing.T) {
	blogSchema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:        blogQuery,
		Subscription: blogSubscription,
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}

	if blogSchema.SubscriptionType() != blogSubscription {
		t.Fatalf("expected blogSchema.SubscriptionType() == blogSubscription")
	}

	subMutation, _ := blogSubscription.Fields()["articleSubscribe"]
	if subMutation == nil {
		t.Fatalf("subMutation is nil")
	}
	subMutationType := subMutation.Type
	if subMutationType != blogArticle {
		t.Fatalf("subMutationType expected to equal blogArticle, got: %v", subMutationType)
	}
	if subMutationType.Name() != "Article" {
		t.Fatalf("subMutationType.Name expected to equal `Article`, got: %v", subMutationType.Name())
	}
	if subMutation.Name != "articleSubscribe" {
		t.Fatalf("subMutation.Name expected to equal `articleSubscribe`, got: %v", subMutation.Name)
	}
}

func TestTypeSystem_DefinitionExample_IncludesNestedInputObjectsInTheMap(t *testing.T) {
	nestedInputObject := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "NestedInputObject",
		Fields: graphql.InputObjectConfigFieldMap{
			"value": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
		},
	})
	someInputObject := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SomeInputObject",
		Fields: graphql.InputObjectConfigFieldMap{
			"nested": &graphql.InputObjectFieldConfig{
				Type: nestedInputObject,
			},
		},
	})
	someMutation := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeMutation",
		Fields: graphql.Fields{
			"mutateSomething": &graphql.Field{
				Type: blogArticle,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: someInputObject,
					},
				},
			},
		},
	})
	someSubscription := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeSubscription",
		Fields: graphql.Fields{
			"subscribeToSomething": &graphql.Field{
				Type: blogArticle,
				Args: graphql.FieldConfigArgument{
					"input": &graphql.ArgumentConfig{
						Type: someInputObject,
					},
				},
			},
		},
	})
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:        blogQuery,
		Mutation:     someMutation,
		Subscription: someSubscription,
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}
	if schema.Type("NestedInputObject") != nestedInputObject {
		t.Fatalf(`schema.GetType("NestedInputObject") expected to equal nestedInputObject, got: %v`, schema.Type("NestedInputObject"))
	}
}

func TestTypeSystem_DefinitionExample_IncludesInterfacesSubTypesInTheTypeMap(t *testing.T) {

	someInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "SomeInterface",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.Int,
			},
		},
	})

	someSubType := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeSubtype",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.Int,
			},
		},
		Interfaces: []*graphql.Interface{someInterface},
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
	})
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"iface": &graphql.Field{
					Type: someInterface,
				},
			},
		}),
		Types: []graphql.Type{someSubType},
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}
	if schema.Type("SomeSubtype") != someSubType {
		t.Fatalf(`schema.GetType("SomeSubtype") expected to equal someSubType, got: %v`, schema.Type("SomeSubtype"))
	}
}

func TestTypeSystem_DefinitionExample_IncludesInterfacesThunkSubtypesInTheTypeMap(t *testing.T) {

	someInterface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "SomeInterface",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.Int,
			},
		},
	})

	someSubType := graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeSubtype",
		Fields: graphql.Fields{
			"f": &graphql.Field{
				Type: graphql.Int,
			},
		},
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{someInterface}
		}),
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
	})
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"iface": &graphql.Field{
					Type: someInterface,
				},
			},
		}),
		Types: []graphql.Type{someSubType},
	})
	if err != nil {
		t.Fatalf("unexpected error, got: %v", err)
	}
	if schema.Type("SomeSubtype") != someSubType {
		t.Fatalf(`schema.GetType("SomeSubtype") expected to equal someSubType, got: %v`, schema.Type("SomeSubtype"))
	}
}

func TestTypeSystem_DefinitionExample_StringifiesSimpleTypes(t *testing.T) {

	type Test struct {
		ttype    graphql.Type
		expected string
	}
	tests := []Test{
		{graphql.Int, "Int"},
		{blogArticle, "Article"},
		{interfaceType, "Interface"},
		{unionType, "Union"},
		{enumType, "Enum"},
		{inputObjectType, "InputObject"},
		{graphql.NewNonNull(graphql.Int), "Int!"},
		{graphql.NewList(graphql.Int), "[Int]"},
		{graphql.NewNonNull(graphql.NewList(graphql.Int)), "[Int]!"},
		{graphql.NewList(graphql.NewNonNull(graphql.Int)), "[Int!]"},
		{graphql.NewList(graphql.NewList(graphql.Int)), "[[Int]]"},
	}
	for _, test := range tests {
		ttypeStr := fmt.Sprintf("%v", test.ttype)
		if ttypeStr != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
	}
}

func TestTypeSystem_DefinitionExample_IdentifiesInputTypes(t *testing.T) {
	type Test struct {
		ttype    graphql.Type
		expected bool
	}
	tests := []Test{
		{graphql.Int, true},
		{objectType, false},
		{interfaceType, false},
		{unionType, false},
		{enumType, true},
		{inputObjectType, true},
	}
	for _, test := range tests {
		ttypeStr := fmt.Sprintf("%v", test.ttype)
		if graphql.IsInputType(test.ttype) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
		if graphql.IsInputType(graphql.NewList(test.ttype)) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
		if graphql.IsInputType(graphql.NewNonNull(test.ttype)) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
	}
}

func TestTypeSystem_DefinitionExample_IdentifiesOutputTypes(t *testing.T) {
	type Test struct {
		ttype    graphql.Type
		expected bool
	}
	tests := []Test{
		{graphql.Int, true},
		{objectType, true},
		{interfaceType, true},
		{unionType, true},
		{enumType, true},
		{inputObjectType, false},
	}
	for _, test := range tests {
		ttypeStr := fmt.Sprintf("%v", test.ttype)
		if graphql.IsOutputType(test.ttype) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
		if graphql.IsOutputType(graphql.NewList(test.ttype)) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
		if graphql.IsOutputType(graphql.NewNonNull(test.ttype)) != test.expected {
			t.Fatalf(`expected %v , got: %v`, test.expected, ttypeStr)
		}
	}
}

func TestTypeSystem_DefinitionExample_ProhibitsNestingNonNullInsideNonNull(t *testing.T) {
	ttype := graphql.NewNonNull(graphql.NewNonNull(graphql.Int))
	expected := `Can only create NonNull of a Nullable Type but got: Int!.`
	if ttype.Error().Error() != expected {
		t.Fatalf(`expected %v , got: %v`, expected, ttype.Error())
	}
}
func TestTypeSystem_DefinitionExample_ProhibitsNilInNonNull(t *testing.T) {
	ttype := graphql.NewNonNull(nil)
	expected := `Can only create NonNull of a Nullable Type but got: <nil>.`
	if ttype.Error().Error() != expected {
		t.Fatalf(`expected %v , got: %v`, expected, ttype.Error())
	}
}
func TestTypeSystem_DefinitionExample_ProhibitsNilTypeInUnions(t *testing.T) {
	ttype := graphql.NewUnion(graphql.UnionConfig{
		Name:  "BadUnion",
		Types: []*graphql.Object{nil},
	})
	expected := `BadUnion may only contain Object types, it cannot contain: <nil>.`
	if ttype.Error().Error() != expected {
		t.Fatalf(`expected %v , got: %v`, expected, ttype.Error())
	}
}
func TestTypeSystem_DefinitionExample_DoesNotMutatePassedFieldDefinitions(t *testing.T) {
	fields := graphql.Fields{
		"field1": &graphql.Field{
			Type: graphql.String,
		},
		"field2": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
		},
	}
	testObject1 := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Test1",
		Fields: fields,
	})
	testObject2 := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Test2",
		Fields: fields,
	})
	if !reflect.DeepEqual(testObject1.Fields(), testObject2.Fields()) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(testObject1.Fields(), testObject2.Fields()))
	}

	expectedFields := graphql.Fields{
		"field1": &graphql.Field{
			Type: graphql.String,
		},
		"field2": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
		},
	}
	if !reflect.DeepEqual(fields, expectedFields) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedFields, fields))
	}

	inputFields := graphql.InputObjectConfigFieldMap{
		"field1": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
		"field2": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
	}
	expectedInputFields := graphql.InputObjectConfigFieldMap{
		"field1": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
		"field2": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
	}
	testInputObject1 := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "Test1",
		Fields: inputFields,
	})
	testInputObject2 := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "Test2",
		Fields: inputFields,
	})
	if !reflect.DeepEqual(testInputObject1.Fields(), testInputObject2.Fields()) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(testInputObject1.Fields(), testInputObject2.Fields()))
	}
	if !reflect.DeepEqual(inputFields, expectedInputFields) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedInputFields, fields))
	}
}

func TestTypeSystem_DefinitionExample_IncludesFieldsThunk(t *testing.T) {
	var someObject *graphql.Object
	someObject = graphql.NewObject(graphql.ObjectConfig{
		Name: "SomeObject",
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			return graphql.Fields{
				"f": &graphql.Field{
					Type: graphql.Int,
				},
				"s": &graphql.Field{
					Type: someObject,
				},
			}
		}),
	})
	fieldMap := someObject.Fields()
	if !reflect.DeepEqual(fieldMap["s"].Type, someObject) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(fieldMap["s"].Type, someObject))
	}
}
