package graphql

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/kinds"
	"github.com/attic-labs/graphql/language/printer"
	"sort"
)

// Prepares an object map of variableValues of the correct type based on the
// provided variable definitions and arbitrary input. If the input cannot be
// parsed to match the variable definitions, a GraphQLError will be returned.
func getVariableValues(schema Schema, definitionASTs []*ast.VariableDefinition, inputs map[string]interface{}) (map[string]interface{}, error) {
	values := map[string]interface{}{}
	for _, defAST := range definitionASTs {
		if defAST == nil || defAST.Variable == nil || defAST.Variable.Name == nil {
			continue
		}
		varName := defAST.Variable.Name.Value
		varValue, err := getVariableValue(schema, defAST, inputs[varName])
		if err != nil {
			return values, err
		}
		values[varName] = varValue
	}
	return values, nil
}

// Prepares an object map of argument values given a list of argument
// definitions and list of argument AST nodes.
func getArgumentValues(argDefs []*Argument, argASTs []*ast.Argument, variableVariables map[string]interface{}) (map[string]interface{}, error) {

	argASTMap := map[string]*ast.Argument{}
	for _, argAST := range argASTs {
		if argAST.Name != nil {
			argASTMap[argAST.Name.Value] = argAST
		}
	}
	results := map[string]interface{}{}
	for _, argDef := range argDefs {

		name := argDef.PrivateName
		var valueAST ast.Value
		if argAST, ok := argASTMap[name]; ok {
			valueAST = argAST.Value
		}
		value := valueFromAST(valueAST, argDef.Type, variableVariables)
		if isNullish(value) {
			value = argDef.DefaultValue
		}
		if !isNullish(value) {
			results[name] = value
		}
	}
	return results, nil
}

// Given a variable definition, and any value of input, return a value which
// adheres to the variable definition, or throw an error.
func getVariableValue(schema Schema, definitionAST *ast.VariableDefinition, input interface{}) (interface{}, error) {
	ttype, err := typeFromAST(schema, definitionAST.Type)
	if err != nil {
		return nil, err
	}
	variable := definitionAST.Variable

	if ttype == nil || !IsInputType(ttype) {
		return "", gqlerrors.NewError(
			fmt.Sprintf(`Variable "$%v" expected value of type `+
				`"%v" which cannot be used as an input type.`, variable.Name.Value, printer.Print(definitionAST.Type)),
			[]ast.Node{definitionAST},
			"",
			nil,
			[]int{},
			nil,
		)
	}

	isValid, messages := isValidInputValue(input, ttype)
	if isValid {
		if isNullish(input) {
			defaultValue := definitionAST.DefaultValue
			if defaultValue != nil {
				variables := map[string]interface{}{}
				val := valueFromAST(defaultValue, ttype, variables)
				return val, nil
			}
		}
		return coerceValue(ttype, input), nil
	}
	if isNullish(input) {
		return "", gqlerrors.NewError(
			fmt.Sprintf(`Variable "$%v" of required type `+
				`"%v" was not provided.`, variable.Name.Value, printer.Print(definitionAST.Type)),
			[]ast.Node{definitionAST},
			"",
			nil,
			[]int{},
			nil,
		)
	}
	// convert input interface into string for error message
	inputStr := ""
	b, err := json.Marshal(input)
	if err == nil {
		inputStr = string(b)
	}
	messagesStr := ""
	if len(messages) > 0 {
		messagesStr = "\n" + strings.Join(messages, "\n")
	}

	return "", gqlerrors.NewError(
		fmt.Sprintf(`Variable "$%v" got invalid value `+
			`%v.%v`, variable.Name.Value, inputStr, messagesStr),
		[]ast.Node{definitionAST},
		"",
		nil,
		[]int{},
		nil,
	)
}

// Given a type and any value, return a runtime value coerced to match the type.
func coerceValue(ttype Input, value interface{}) interface{} {
	if ttype, ok := ttype.(*NonNull); ok {
		return coerceValue(ttype.OfType, value)
	}
	if isNullish(value) {
		return nil
	}
	if ttype, ok := ttype.(*List); ok {
		itemType := ttype.OfType
		valType := reflect.ValueOf(value)
		if valType.Kind() == reflect.Slice {
			values := []interface{}{}
			for i := 0; i < valType.Len(); i++ {
				val := valType.Index(i).Interface()
				v := coerceValue(itemType, val)
				values = append(values, v)
			}
			return values
		}
		val := coerceValue(itemType, value)
		return []interface{}{val}
	}
	if ttype, ok := ttype.(*InputObject); ok {

		valueMap, ok := value.(map[string]interface{})
		if !ok {
			valueMap = map[string]interface{}{}
		}

		obj := map[string]interface{}{}
		for fieldName, field := range ttype.Fields() {
			value, _ := valueMap[fieldName]
			fieldValue := coerceValue(field.Type, value)
			if isNullish(fieldValue) {
				fieldValue = field.DefaultValue
			}
			if !isNullish(fieldValue) {
				obj[fieldName] = fieldValue
			}
		}
		return obj
	}

	switch ttype := ttype.(type) {
	case *Scalar:
		parsed := ttype.ParseValue(value)
		if !isNullish(parsed) {
			return parsed
		}
	case *Enum:
		parsed := ttype.ParseValue(value)
		if !isNullish(parsed) {
			return parsed
		}
	}
	return nil
}

// graphql-js/src/utilities.js`
// TODO: figure out where to organize utils
// TODO: change to *Schema
func typeFromAST(schema Schema, inputTypeAST ast.Type) (Type, error) {
	switch inputTypeAST := inputTypeAST.(type) {
	case *ast.List:
		innerType, err := typeFromAST(schema, inputTypeAST.Type)
		if err != nil {
			return nil, err
		}
		return NewList(innerType), nil
	case *ast.NonNull:
		innerType, err := typeFromAST(schema, inputTypeAST.Type)
		if err != nil {
			return nil, err
		}
		return NewNonNull(innerType), nil
	case *ast.Named:
		nameValue := ""
		if inputTypeAST.Name != nil {
			nameValue = inputTypeAST.Name.Value
		}
		ttype := schema.Type(nameValue)
		if ttype == nil {
			return nil, fmt.Errorf("Type %s not found", nameValue)
		}
		return ttype, nil
	default:
		return nil, invariant(inputTypeAST.GetKind() == kinds.Named, "Must be a named type.")
	}
}

// isValidInputValue alias isValidJSValue
// Given a value and a GraphQL type, determine if the value will be
// accepted for that type. This is primarily useful for validating the
// runtime values of query variables.
func isValidInputValue(value interface{}, ttype Input) (bool, []string) {
	if ttype, ok := ttype.(*NonNull); ok {
		if isNullish(value) {
			if ttype.OfType.Name() != "" {
				return false, []string{fmt.Sprintf(`Expected "%v!", found null.`, ttype.OfType.Name())}
			}
			return false, []string{"Expected non-null value, found null."}
		}
		return isValidInputValue(value, ttype.OfType)
	}

	if isNullish(value) {
		return true, nil
	}

	switch ttype := ttype.(type) {
	case *List:
		itemType := ttype.OfType
		valType := reflect.ValueOf(value)
		if valType.Kind() == reflect.Ptr {
			valType = valType.Elem()
		}
		if valType.Kind() == reflect.Slice {
			messagesReduce := []string{}
			for i := 0; i < valType.Len(); i++ {
				val := valType.Index(i).Interface()
				_, messages := isValidInputValue(val, itemType)
				for idx, message := range messages {
					messagesReduce = append(messagesReduce, fmt.Sprintf(`In element #%v: %v`, idx+1, message))
				}
			}
			return (len(messagesReduce) == 0), messagesReduce
		}
		return isValidInputValue(value, itemType)

	case *InputObject:
		messagesReduce := []string{}

		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return false, []string{fmt.Sprintf(`Expected "%v", found not an object.`, ttype.Name())}
		}
		fields := ttype.Fields()

		// to ensure stable order of field evaluation
		fieldNames := []string{}
		valueMapFieldNames := []string{}

		for fieldName := range fields {
			fieldNames = append(fieldNames, fieldName)
		}
		sort.Strings(fieldNames)

		for fieldName := range valueMap {
			valueMapFieldNames = append(valueMapFieldNames, fieldName)
		}
		sort.Strings(valueMapFieldNames)

		// Ensure every provided field is defined.
		for _, fieldName := range valueMapFieldNames {
			if _, ok := fields[fieldName]; !ok {
				messagesReduce = append(messagesReduce, fmt.Sprintf(`In field "%v": Unknown field.`, fieldName))
			}
		}

		// Ensure every defined field is valid.
		for _, fieldName := range fieldNames {
			_, messages := isValidInputValue(valueMap[fieldName], fields[fieldName].Type)
			if messages != nil {
				for _, message := range messages {
					messagesReduce = append(messagesReduce, fmt.Sprintf(`In field "%v": %v`, fieldName, message))
				}
			}
		}
		return (len(messagesReduce) == 0), messagesReduce
	}

	switch ttype := ttype.(type) {
	case *Scalar:
		parsedVal := ttype.ParseValue(value)
		if isNullish(parsedVal) {
			return false, []string{fmt.Sprintf(`Expected type "%v", found "%v".`, ttype.Name(), value)}
		}
		return true, nil

	case *Enum:
		parsedVal := ttype.ParseValue(value)
		if isNullish(parsedVal) {
			return false, []string{fmt.Sprintf(`Expected type "%v", found "%v".`, ttype.Name(), value)}
		}
		return true, nil
	}
	return true, nil
}

// Returns true if a value is null, undefined, or NaN.
func isNullish(value interface{}) bool {
	// https://github.com/graphql-go/graphql/issues/183
	// if value, ok := value.(string); ok {
	// 	return value == ""
	// }
	if value, ok := value.(int); ok {
		return math.IsNaN(float64(value))
	}
	if value, ok := value.(float32); ok {
		return math.IsNaN(float64(value))
	}
	if value, ok := value.(float64); ok {
		return math.IsNaN(value)
	}
	return value == nil
}

/**
 * Produces a value given a GraphQL Value AST.
 *
 * A GraphQL type must be provided, which will be used to interpret different
 * GraphQL Value literals.
 *
 * | GraphQL Value        | JSON Value    |
 * | -------------------- | ------------- |
 * | Input Object         | Object        |
 * | List                 | Array         |
 * | Boolean              | Boolean       |
 * | String / Enum Value  | String        |
 * | Int / Float          | Number        |
 *
 */
func valueFromAST(valueAST ast.Value, ttype Input, variables map[string]interface{}) interface{} {

	if ttype, ok := ttype.(*NonNull); ok {
		val := valueFromAST(valueAST, ttype.OfType, variables)
		return val
	}

	if valueAST == nil {
		return nil
	}

	if valueAST, ok := valueAST.(*ast.Variable); ok && valueAST.Kind == kinds.Variable {
		if valueAST.Name == nil {
			return nil
		}
		if variables == nil {
			return nil
		}
		variableName := valueAST.Name.Value
		variableVal, ok := variables[variableName]
		if !ok {
			return nil
		}
		// Note: we're not doing any checking that this variable is correct. We're
		// assuming that this query has been validated and the variable usage here
		// is of the correct type.
		return variableVal
	}

	if ttype, ok := ttype.(*List); ok {
		itemType := ttype.OfType
		if valueAST, ok := valueAST.(*ast.ListValue); ok && valueAST.Kind == kinds.ListValue {
			values := []interface{}{}
			for _, itemAST := range valueAST.Values {
				v := valueFromAST(itemAST, itemType, variables)
				values = append(values, v)
			}
			return values
		}
		v := valueFromAST(valueAST, itemType, variables)
		return []interface{}{v}
	}

	if ttype, ok := ttype.(*InputObject); ok {
		valueAST, ok := valueAST.(*ast.ObjectValue)
		if !ok {
			return nil
		}
		fieldASTs := map[string]*ast.ObjectField{}
		for _, fieldAST := range valueAST.Fields {
			if fieldAST.Name == nil {
				continue
			}
			fieldName := fieldAST.Name.Value
			fieldASTs[fieldName] = fieldAST

		}
		obj := map[string]interface{}{}
		for fieldName, field := range ttype.Fields() {
			fieldAST, ok := fieldASTs[fieldName]
			if !ok || fieldAST == nil {
				continue
			}
			fieldValue := valueFromAST(fieldAST.Value, field.Type, variables)
			if isNullish(fieldValue) {
				fieldValue = field.DefaultValue
			}
			if !isNullish(fieldValue) {
				obj[fieldName] = fieldValue
			}
		}
		return obj
	}

	switch ttype := ttype.(type) {
	case *Scalar:
		parsed := ttype.ParseLiteral(valueAST)
		if !isNullish(parsed) {
			return parsed
		}
	case *Enum:
		parsed := ttype.ParseLiteral(valueAST)
		if !isNullish(parsed) {
			return parsed
		}
	}
	return nil
}

func invariant(condition bool, message string) error {
	if !condition {
		return gqlerrors.NewFormattedError(message)
	}
	return nil
}
