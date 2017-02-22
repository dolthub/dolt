package testutil

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/location"
	"github.com/attic-labs/graphql/language/parser"
	"github.com/attic-labs/graphql/language/source"
	"reflect"
)

var TestSchema *graphql.Schema

func init() {

	var beingInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Being",
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
		},
	})
	var petInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Pet",
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
		},
	})
	var canineInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Canine",
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
		},
	})
	var dogCommandEnum = graphql.NewEnum(graphql.EnumConfig{
		Name: "DogCommand",
		Values: graphql.EnumValueConfigMap{
			"SIT": &graphql.EnumValueConfig{
				Value: 0,
			},
			"HEEL": &graphql.EnumValueConfig{
				Value: 1,
			},
			"DOWN": &graphql.EnumValueConfig{
				Value: 2,
			},
		},
	})
	var dogType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Dog",
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
			"nickname": &graphql.Field{
				Type: graphql.String,
			},
			"barkVolume": &graphql.Field{
				Type: graphql.Int,
			},
			"barks": &graphql.Field{
				Type: graphql.Boolean,
			},
			"doesKnowCommand": &graphql.Field{
				Type: graphql.Boolean,
				Args: graphql.FieldConfigArgument{
					"dogCommand": &graphql.ArgumentConfig{
						Type: dogCommandEnum,
					},
				},
			},
			"isHousetrained": &graphql.Field{
				Type: graphql.Boolean,
				Args: graphql.FieldConfigArgument{
					"atOtherHomes": &graphql.ArgumentConfig{
						Type:         graphql.Boolean,
						DefaultValue: true,
					},
				},
			},
			"isAtLocation": &graphql.Field{
				Type: graphql.Boolean,
				Args: graphql.FieldConfigArgument{
					"x": &graphql.ArgumentConfig{
						Type: graphql.Int,
					},
					"y": &graphql.ArgumentConfig{
						Type: graphql.Int,
					},
				},
			},
		},
		Interfaces: []*graphql.Interface{
			beingInterface,
			petInterface,
			canineInterface,
		},
	})
	var furColorEnum = graphql.NewEnum(graphql.EnumConfig{
		Name: "FurColor",
		Values: graphql.EnumValueConfigMap{
			"BROWN": &graphql.EnumValueConfig{
				Value: 0,
			},
			"BLACK": &graphql.EnumValueConfig{
				Value: 1,
			},
			"TAN": &graphql.EnumValueConfig{
				Value: 2,
			},
			"SPOTTED": &graphql.EnumValueConfig{
				Value: 3,
			},
		},
	})

	var catType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Cat",
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
			"nickname": &graphql.Field{
				Type: graphql.String,
			},
			"meowVolume": &graphql.Field{
				Type: graphql.Int,
			},
			"meows": &graphql.Field{
				Type: graphql.Boolean,
			},
			"furColor": &graphql.Field{
				Type: furColorEnum,
			},
		},
		Interfaces: []*graphql.Interface{
			beingInterface,
			petInterface,
		},
	})
	var catOrDogUnion = graphql.NewUnion(graphql.UnionConfig{
		Name: "CatOrDog",
		Types: []*graphql.Object{
			dogType,
			catType,
		},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			// not used for validation
			return nil
		},
	})
	var intelligentInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "Intelligent",
		Fields: graphql.Fields{
			"iq": &graphql.Field{
				Type: graphql.Int,
			},
		},
	})

	var humanType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Human",
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Interfaces: []*graphql.Interface{
			beingInterface,
			intelligentInterface,
		},
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
			"pets": &graphql.Field{
				Type: graphql.NewList(petInterface),
			},
			"iq": &graphql.Field{
				Type: graphql.Int,
			},
		},
	})

	humanType.AddFieldConfig("relatives", &graphql.Field{
		Type: graphql.NewList(humanType),
	})

	var alienType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Alien",
		IsTypeOf: func(p graphql.IsTypeOfParams) bool {
			return true
		},
		Interfaces: []*graphql.Interface{
			beingInterface,
			intelligentInterface,
		},
		Fields: graphql.Fields{
			"name": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"surname": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
			"iq": &graphql.Field{
				Type: graphql.Int,
			},
			"numEyes": &graphql.Field{
				Type: graphql.Int,
			},
		},
	})
	var dogOrHumanUnion = graphql.NewUnion(graphql.UnionConfig{
		Name: "DogOrHuman",
		Types: []*graphql.Object{
			dogType,
			humanType,
		},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			// not used for validation
			return nil
		},
	})
	var humanOrAlienUnion = graphql.NewUnion(graphql.UnionConfig{
		Name: "HumanOrAlien",
		Types: []*graphql.Object{
			alienType,
			humanType,
		},
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			// not used for validation
			return nil
		},
	})

	var complexInputObject = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ComplexInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"requiredField": &graphql.InputObjectFieldConfig{
				Type: graphql.NewNonNull(graphql.Boolean),
			},
			"intField": &graphql.InputObjectFieldConfig{
				Type: graphql.Int,
			},
			"stringField": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"booleanField": &graphql.InputObjectFieldConfig{
				Type: graphql.Boolean,
			},
			"stringListField": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.String),
			},
		},
	})
	var complicatedArgs = graphql.NewObject(graphql.ObjectConfig{
		Name: "ComplicatedArgs",
		// TODO List
		// TODO Coercion
		// TODO NotNulls
		Fields: graphql.Fields{
			"intArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"intArg": &graphql.ArgumentConfig{
						Type: graphql.Int,
					},
				},
			},
			"nonNullIntArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"nonNullIntArg": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
				},
			},
			"stringArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"stringArg": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
			},
			"booleanArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"booleanArg": &graphql.ArgumentConfig{
						Type: graphql.Boolean,
					},
				},
			},
			"enumArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"enumArg": &graphql.ArgumentConfig{
						Type: furColorEnum,
					},
				},
			},
			"floatArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"floatArg": &graphql.ArgumentConfig{
						Type: graphql.Float,
					},
				},
			},
			"idArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"idArg": &graphql.ArgumentConfig{
						Type: graphql.ID,
					},
				},
			},
			"stringListArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"stringListArg": &graphql.ArgumentConfig{
						Type: graphql.NewList(graphql.String),
					},
				},
			},
			"complexArgField": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"complexArg": &graphql.ArgumentConfig{
						Type: complexInputObject,
					},
				},
			},
			"multipleReqs": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"req1": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
					"req2": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
				},
			},
			"multipleOpts": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"opt1": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: 0,
					},
					"opt2": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: 0,
					},
				},
			},
			"multipleOptAndReq": &graphql.Field{
				Type: graphql.String,
				Args: graphql.FieldConfigArgument{
					"req1": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
					"req2": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
					"opt1": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: 0,
					},
					"opt2": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: 0,
					},
				},
			},
		},
	})
	queryRoot := graphql.NewObject(graphql.ObjectConfig{
		Name: "QueryRoot",
		Fields: graphql.Fields{
			"human": &graphql.Field{
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Type: graphql.ID,
					},
				},
				Type: humanType,
			},
			"alien": &graphql.Field{
				Type: alienType,
			},
			"dog": &graphql.Field{
				Type: dogType,
			},
			"cat": &graphql.Field{
				Type: catType,
			},
			"pet": &graphql.Field{
				Type: petInterface,
			},
			"catOrDog": &graphql.Field{
				Type: catOrDogUnion,
			},
			"dogOrHuman": &graphql.Field{
				Type: dogOrHumanUnion,
			},
			"humanOrAlien": &graphql.Field{
				Type: humanOrAlienUnion,
			},
			"complicatedArgs": &graphql.Field{
				Type: complicatedArgs,
			},
		},
	})
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: queryRoot,
		Directives: []*graphql.Directive{
			graphql.NewDirective(graphql.DirectiveConfig{
				Name:      "operationOnly",
				Locations: []string{graphql.DirectiveLocationQuery},
			}),
			graphql.IncludeDirective,
			graphql.SkipDirective,
		},
		Types: []graphql.Type{
			catType,
			dogType,
			humanType,
			alienType,
		},
	})
	if err != nil {
		panic(err)
	}
	TestSchema = &schema

}
func expectValidRule(t *testing.T, schema *graphql.Schema, rules []graphql.ValidationRuleFn, queryString string) {
	source := source.NewSource(&source.Source{
		Body: []byte(queryString),
	})
	AST, err := parser.Parse(parser.ParseParams{Source: source})
	if err != nil {
		t.Fatal(err)
	}
	result := graphql.ValidateDocument(schema, AST, rules)
	if len(result.Errors) > 0 {
		t.Fatalf("Should validate, got %v", result.Errors)
	}
	if result.IsValid != true {
		t.Fatalf("IsValid should be true, got %v", result.IsValid)
	}

}
func expectInvalidRule(t *testing.T, schema *graphql.Schema, rules []graphql.ValidationRuleFn, queryString string, expectedErrors []gqlerrors.FormattedError) {
	source := source.NewSource(&source.Source{
		Body: []byte(queryString),
	})
	AST, err := parser.Parse(parser.ParseParams{Source: source})
	if err != nil {
		t.Fatal(err)
	}
	result := graphql.ValidateDocument(schema, AST, rules)
	if len(result.Errors) != len(expectedErrors) {
		t.Fatalf("Should have %v errors, got %v", len(expectedErrors), len(result.Errors))
	}
	if result.IsValid != false {
		t.Fatalf("IsValid should be false, got %v", result.IsValid)
	}
	for _, expectedErr := range expectedErrors {
		found := false
		for _, err := range result.Errors {
			if reflect.DeepEqual(expectedErr, err) {
				found = true
				break
			}
		}
		if found == false {
			t.Fatalf("Unexpected result, Diff: %v", Diff(expectedErrors, result.Errors))
		}
	}

}
func ExpectPassesRule(t *testing.T, rule graphql.ValidationRuleFn, queryString string) {
	expectValidRule(t, TestSchema, []graphql.ValidationRuleFn{rule}, queryString)
}
func ExpectFailsRule(t *testing.T, rule graphql.ValidationRuleFn, queryString string, expectedErrors []gqlerrors.FormattedError) {
	expectInvalidRule(t, TestSchema, []graphql.ValidationRuleFn{rule}, queryString, expectedErrors)
}
func ExpectFailsRuleWithSchema(t *testing.T, schema *graphql.Schema, rule graphql.ValidationRuleFn, queryString string, expectedErrors []gqlerrors.FormattedError) {
	expectInvalidRule(t, schema, []graphql.ValidationRuleFn{rule}, queryString, expectedErrors)
}
func ExpectPassesRuleWithSchema(t *testing.T, schema *graphql.Schema, rule graphql.ValidationRuleFn, queryString string) {
	expectValidRule(t, schema, []graphql.ValidationRuleFn{rule}, queryString)
}
func RuleError(message string, locs ...int) gqlerrors.FormattedError {
	locations := []location.SourceLocation{}
	for i := 0; i < len(locs); i = i + 2 {
		line := locs[i]
		col := 0
		if i+1 < len(locs) {
			col = locs[i+1]
		}
		locations = append(locations, location.SourceLocation{
			Line:   line,
			Column: col,
		})
	}
	return gqlerrors.FormattedError{
		Message:   message,
		Locations: locations,
	}
}
