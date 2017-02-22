package graphql_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/location"
	"github.com/attic-labs/graphql/testutil"
)

var testComplexScalar *graphql.Scalar = graphql.NewScalar(graphql.ScalarConfig{
	Name: "ComplexScalar",
	Serialize: func(value interface{}) interface{} {
		if value == "DeserializedValue" {
			return "SerializedValue"
		}
		return nil
	},
	ParseValue: func(value interface{}) interface{} {
		if value == "SerializedValue" {
			return "DeserializedValue"
		}
		return nil
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		astValue := valueAST.GetValue()
		if astValue, ok := astValue.(string); ok && astValue == "SerializedValue" {
			return "DeserializedValue"
		}
		return nil
	},
})

var testInputObject *graphql.InputObject = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "TestInputObject",
	Fields: graphql.InputObjectConfigFieldMap{
		"a": &graphql.InputObjectFieldConfig{
			Type: graphql.String,
		},
		"b": &graphql.InputObjectFieldConfig{
			Type: graphql.NewList(graphql.String),
		},
		"c": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.String),
		},
		"d": &graphql.InputObjectFieldConfig{
			Type: testComplexScalar,
		},
	},
})

var testNestedInputObject *graphql.InputObject = graphql.NewInputObject(graphql.InputObjectConfig{
	Name: "TestNestedInputObject",
	Fields: graphql.InputObjectConfigFieldMap{
		"na": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(testInputObject),
		},
		"nb": &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(graphql.String),
		},
	},
})

func inputResolved(p graphql.ResolveParams) (interface{}, error) {
	input, ok := p.Args["input"]
	if !ok {
		return nil, nil
	}
	b, err := json.Marshal(input)
	if err != nil {
		return nil, nil
	}
	return string(b), nil
}

var testType *graphql.Object = graphql.NewObject(graphql.ObjectConfig{
	Name: "TestType",
	Fields: graphql.Fields{
		"fieldWithObjectInput": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: testInputObject,
				},
			},
			Resolve: inputResolved,
		},
		"fieldWithNullableStringInput": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
			Resolve: inputResolved,
		},
		"fieldWithNonNullableStringInput": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.String),
				},
			},
			Resolve: inputResolved,
		},
		"fieldWithDefaultArgumentValue": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type:         graphql.String,
					DefaultValue: "Hello World",
				},
			},
			Resolve: inputResolved,
		},
		"fieldWithNestedInputObject": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type:         testNestedInputObject,
					DefaultValue: "Hello World",
				},
			},
			Resolve: inputResolved,
		},
		"list": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewList(graphql.String),
				},
			},
			Resolve: inputResolved,
		},
		"nnList": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
				},
			},
			Resolve: inputResolved,
		},
		"listNN": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
				},
			},
			Resolve: inputResolved,
		},
		"nnListNN": &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String))),
				},
			},
			Resolve: inputResolved,
		},
	},
})

var variablesTestSchema, _ = graphql.NewSchema(graphql.SchemaConfig{
	Query: testType,
})

func TestVariables_ObjectsAndNullability_UsingInlineStructs_ExecutesWithComplexInput(t *testing.T) {
	doc := `
        {
          fieldWithObjectInput(input: {a: "foo", b: ["bar"], c: "baz"})
        }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","b":["bar"],"c":"baz"}`,
		},
	}
	// parse query
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingInlineStructs_ProperlyParsesSingleValueToList(t *testing.T) {
	doc := `
        {
          fieldWithObjectInput(input: {a: "foo", b: "bar", c: "baz"})
        }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","b":["bar"],"c":"baz"}`,
		},
	}
	// parse query
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingInlineStructs_DoesNotUseIncorrectValue(t *testing.T) {
	doc := `
        {
          fieldWithObjectInput(input: ["foo", "bar", "baz"])
        }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": nil,
		},
	}
	// parse query
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingInlineStructs_ProperlyRunsParseLiteralOnComplexScalarTypes(t *testing.T) {
	doc := `
        {
          fieldWithObjectInput(input: {a: "foo", d: "SerializedValue"})
        }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","d":"DeserializedValue"}`,
		},
	}
	// parse query
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func testVariables_ObjectsAndNullability_UsingVariables_GetAST(t *testing.T) *ast.Document {
	doc := `
        query q($input: TestInputObject) {
          fieldWithObjectInput(input: $input)
        }
	`
	return testutil.TestParse(t, doc)
}
func TestVariables_ObjectsAndNullability_UsingVariables_ExecutesWithComplexInput(t *testing.T) {

	params := map[string]interface{}{
		"input": map[string]interface{}{
			"a": "foo",
			"b": []interface{}{"bar"},
			"c": "baz",
		},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","b":["bar"],"c":"baz"}`,
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func TestVariables_ObjectsAndNullability_UsingVariables_UsesDefaultValueWhenNotProvided(t *testing.T) {

	doc := `
	  query q($input: TestInputObject = {a: "foo", b: ["bar"], c: "baz"}) {
		fieldWithObjectInput(input: $input)
	  }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","b":["bar"],"c":"baz"}`,
		},
	}

	withDefaultsAST := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    withDefaultsAST,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ProperlyParsesSingleValueToList(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"a":"foo","b":["bar"],"c":"baz"}`,
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ExecutesWithComplexScalarInput(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"c": "foo",
			"d": "SerializedValue",
		},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithObjectInput": `{"c":"foo","d":"DeserializedValue"}`,
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ErrorsOnNullForNestedNonNull(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": nil,
		},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value {"a":"foo","b":"bar","c":null}.` +
					"\nIn field \"c\": Expected \"String!\", found null.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ErrorsOnIncorrectType(t *testing.T) {
	params := map[string]interface{}{
		"input": "foo bar",
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: "Variable \"$input\" got invalid value \"foo bar\".\nExpected \"TestInputObject\", found not an object.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ErrorsOnOmissionOfNestedNonNull(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"a": "foo",
			"b": "bar",
		},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value {"a":"foo","b":"bar"}.` +
					"\nIn field \"c\": Expected \"String!\", found null.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ErrorsOnDeepNestedErrorsAndWithManyErrors(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"na": map[string]interface{}{
				"a": "foo",
			},
		},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value {"na":{"a":"foo"}}.` +
					"\nIn field \"na\": In field \"c\": Expected \"String!\", found null." +
					"\nIn field \"nb\": Expected \"String!\", found null.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 19,
					},
				},
			},
		},
	}
	doc := `
          query q($input: TestNestedInputObject) {
            fieldWithNestedObjectInput(input: $input)
          }
        `

	nestedAST := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    nestedAST,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ObjectsAndNullability_UsingVariables_ErrorsOnAdditionOfUnknownInputField(t *testing.T) {
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"a":     "foo",
			"b":     "bar",
			"c":     "baz",
			"extra": "dog",
		},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value {"a":"foo","b":"bar","c":"baz","extra":"dog"}.` +
					"\nIn field \"extra\": Unknown field.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}

	ast := testVariables_ObjectsAndNullability_UsingVariables_GetAST(t)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func TestVariables_NullableScalars_AllowsNullableInputsToBeOmitted(t *testing.T) {
	doc := `
      {
        fieldWithNullableStringInput
      }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": nil,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NullableScalars_AllowsNullableInputsToBeOmittedInAVariable(t *testing.T) {
	doc := `
      query SetsNullable($value: String) {
        fieldWithNullableStringInput(input: $value)
      }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": nil,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NullableScalars_AllowsNullableInputsToBeOmittedInAnUnlistedVariable(t *testing.T) {
	doc := `
      query SetsNullable {
        fieldWithNullableStringInput(input: $value)
      }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": nil,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NullableScalars_AllowsNullableInputsToBeSetToNullInAVariable(t *testing.T) {
	doc := `
      query SetsNullable($value: String) {
        fieldWithNullableStringInput(input: $value)
      }
	`
	params := map[string]interface{}{
		"value": nil,
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": nil,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NullableScalars_AllowsNullableInputsToBeSetToAValueInAVariable(t *testing.T) {
	doc := `
      query SetsNullable($value: String) {
        fieldWithNullableStringInput(input: $value)
      }
	`
	params := map[string]interface{}{
		"value": "a",
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": `"a"`,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NullableScalars_AllowsNullableInputsToBeSetToAValueDirectly(t *testing.T) {
	doc := `
      {
        fieldWithNullableStringInput(input: "a")
      }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNullableStringInput": `"a"`,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func TestVariables_NonNullableScalars_DoesNotAllowNonNullableInputsToBeOmittedInAVariable(t *testing.T) {

	doc := `
        query SetsNonNullable($value: String!) {
          fieldWithNonNullableStringInput(input: $value)
        }
	`

	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$value" of required type "String!" was not provided.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 31,
					},
				},
			},
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NonNullableScalars_DoesNotAllowNonNullableInputsToBeSetToNullInAVariable(t *testing.T) {
	doc := `
        query SetsNonNullable($value: String!) {
          fieldWithNonNullableStringInput(input: $value)
        }
	`

	params := map[string]interface{}{
		"value": nil,
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$value" of required type "String!" was not provided.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 31,
					},
				},
			},
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NonNullableScalars_AllowsNonNullableInputsToBeSetToAValueInAVariable(t *testing.T) {
	doc := `
        query SetsNonNullable($value: String!) {
          fieldWithNonNullableStringInput(input: $value)
        }
	`

	params := map[string]interface{}{
		"value": "a",
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNonNullableStringInput": `"a"`,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NonNullableScalars_AllowsNonNullableInputsToBeSetToAValueDirectly(t *testing.T) {
	doc := `
      {
        fieldWithNonNullableStringInput(input: "a")
      }
	`

	params := map[string]interface{}{
		"value": "a",
	}

	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNonNullableStringInput": `"a"`,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_NonNullableScalars_PassesAlongNullForNonNullableInputsIfExplicitlySetInTheQuery(t *testing.T) {
	doc := `
      {
        fieldWithNonNullableStringInput
      }
	`

	params := map[string]interface{}{
		"value": "a",
	}

	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithNonNullableStringInput": nil,
		},
	}

	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func TestVariables_ListsAndNullability_AllowsListsToBeNull(t *testing.T) {
	doc := `
        query q($input: [String]) {
          list(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": nil,
	}

	expected := &graphql.Result{
		Data: map[string]interface{}{
			"list": nil,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsListsToContainValues(t *testing.T) {
	doc := `
        query q($input: [String]) {
          list(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A"},
	}

	expected := &graphql.Result{
		Data: map[string]interface{}{
			"list": `["A"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsListsToContainNull(t *testing.T) {
	doc := `
        query q($input: [String]) {
          list(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A", nil, "B"},
	}

	expected := &graphql.Result{
		Data: map[string]interface{}{
			"list": `["A",null,"B"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowNonNullListsToBeNull(t *testing.T) {
	doc := `
        query q($input: [String]!) {
          nnList(input: $input)
        }
	`
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" of required type "[String]!" was not provided.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsNonNullListsToContainValues(t *testing.T) {
	doc := `
        query q($input: [String]!) {
          nnList(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A"},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"nnList": `["A"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsNonNullListsToContainNull(t *testing.T) {
	doc := `
        query q($input: [String]!) {
          nnList(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A", nil, "B"},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"nnList": `["A",null,"B"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsListsOfNonNullsToBeNull(t *testing.T) {
	doc := `
        query q($input: [String!]) {
          listNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": nil,
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"listNN": nil,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsListsOfNonNullsToContainValues(t *testing.T) {
	doc := `
        query q($input: [String!]) {
          listNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A"},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"listNN": `["A"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) > 0 {
		t.Fatalf("wrong result, unexpected errors: %v", result.Errors)
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowListOfNonNullsToContainNull(t *testing.T) {
	doc := `
        query q($input: [String!]) {
          listNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A", nil, "B"},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value ` +
					`["A",null,"B"].` +
					"\nIn element #1: Expected \"String!\", found null.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowNonNullListOfNonNullsToBeNull(t *testing.T) {
	doc := `
        query q($input: [String!]!) {
          nnListNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": nil,
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" of required type "[String!]!" was not provided.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_AllowsNonNullListsOfNonNulsToContainValues(t *testing.T) {
	doc := `
        query q($input: [String!]!) {
          nnListNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A"},
	}
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"nnListNN": `["A"]`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowNonNullListOfNonNullsToContainNull(t *testing.T) {
	doc := `
        query q($input: [String!]!) {
          nnListNN(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": []interface{}{"A", nil, "B"},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" got invalid value ` +
					`["A",null,"B"].` +
					"\nIn element #1: Expected \"String!\", found null.",
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowInvalidTypesToBeUsedAsValues(t *testing.T) {
	doc := `
        query q($input: TestType!) {
          fieldWithObjectInput(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": map[string]interface{}{
			"list": []interface{}{"A", "B"},
		},
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" expected value of type "TestType!" which cannot be used as an input type.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_ListsAndNullability_DoesNotAllowUnknownTypesToBeUsedAsValues(t *testing.T) {
	doc := `
        query q($input: UnknownType!) {
          fieldWithObjectInput(input: $input)
        }
	`
	params := map[string]interface{}{
		"input": "whoknows",
	}
	expected := &graphql.Result{
		Data: nil,
		Errors: []gqlerrors.FormattedError{
			{
				Message: `Variable "$input" expected value of type "UnknownType!" which cannot be used as an input type.`,
				Locations: []location.SourceLocation{
					{
						Line: 2, Column: 17,
					},
				},
			},
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
		Args:   params,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}

func TestVariables_UsesArgumentDefaultValues_WhenNoArgumentProvided(t *testing.T) {
	doc := `
	{
      fieldWithDefaultArgumentValue
    }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithDefaultArgumentValue": `"Hello World"`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_UsesArgumentDefaultValues_WhenNullableVariableProvided(t *testing.T) {
	doc := `
	query optionalVariable($optional: String) {
        fieldWithDefaultArgumentValue(input: $optional)
    }
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithDefaultArgumentValue": `"Hello World"`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
func TestVariables_UsesArgumentDefaultValues_WhenArgumentProvidedCannotBeParsed(t *testing.T) {
	doc := `
	{
		fieldWithDefaultArgumentValue(input: WRONG_TYPE)
	}
	`
	expected := &graphql.Result{
		Data: map[string]interface{}{
			"fieldWithDefaultArgumentValue": `"Hello World"`,
		},
	}
	ast := testutil.TestParse(t, doc)

	// execute
	ep := graphql.ExecuteParams{
		Schema: variablesTestSchema,
		AST:    ast,
	}
	result := testutil.TestExecute(t, ep)
	if len(result.Errors) != len(expected.Errors) {
		t.Fatalf("Unexpected errors, Diff: %v", testutil.Diff(expected.Errors, result.Errors))
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, result))
	}
}
