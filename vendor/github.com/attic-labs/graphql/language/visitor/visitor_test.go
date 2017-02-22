package visitor_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"fmt"
	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/kinds"
	"github.com/attic-labs/graphql/language/parser"
	"github.com/attic-labs/graphql/language/printer"
	"github.com/attic-labs/graphql/language/visitor"
	"github.com/attic-labs/graphql/testutil"
)

func parse(t *testing.T, query string) *ast.Document {
	astDoc, err := parser.Parse(parser.ParseParams{
		Source: query,
		Options: parser.ParseOptions{
			NoLocation: true,
		},
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return astDoc
}

func TestVisitor_AllowsEditingANodeBothOnEnterAndOnLeave(t *testing.T) {

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	var selectionSet *ast.SelectionSet

	expectedQuery := `{ a, b, c { a, b, c } }`
	expectedAST := parse(t, expectedQuery)

	visited := map[string]bool{
		"didEnter": false,
		"didLeave": false,
	}

	expectedVisited := map[string]bool{
		"didEnter": true,
		"didLeave": true,
	}

	v := &visitor.VisitorOptions{

		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok {
						selectionSet = node.SelectionSet
						visited["didEnter"] = true
						return visitor.ActionUpdate, ast.NewOperationDefinition(&ast.OperationDefinition{
							Loc:                 node.Loc,
							Operation:           node.Operation,
							Name:                node.Name,
							VariableDefinitions: node.VariableDefinitions,
							Directives:          node.Directives,
							SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
								Selections: []ast.Selection{},
							}),
						})
					}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok {
						visited["didLeave"] = true
						return visitor.ActionUpdate, ast.NewOperationDefinition(&ast.OperationDefinition{
							Loc:                 node.Loc,
							Operation:           node.Operation,
							Name:                node.Name,
							VariableDefinitions: node.VariableDefinitions,
							Directives:          node.Directives,
							SelectionSet:        selectionSet,
						})
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}

	editedAst := visitor.Visit(astDoc, v, nil)
	if !reflect.DeepEqual(expectedAST, editedAst) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedAST, editedAst))
	}

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(visited, expectedVisited))
	}

}
func TestVisitor_AllowsEditingTheRootNodeOnEnterAndOnLeave(t *testing.T) {

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	definitions := astDoc.Definitions

	expectedQuery := `{ a, b, c { a, b, c } }`
	expectedAST := parse(t, expectedQuery)

	visited := map[string]bool{
		"didEnter": false,
		"didLeave": false,
	}

	expectedVisited := map[string]bool{
		"didEnter": true,
		"didLeave": true,
	}

	v := &visitor.VisitorOptions{

		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Document: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Document); ok {
						visited["didEnter"] = true
						return visitor.ActionUpdate, ast.NewDocument(&ast.Document{
							Loc:         node.Loc,
							Definitions: []ast.Node{},
						})
					}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Document); ok {
						visited["didLeave"] = true
						return visitor.ActionUpdate, ast.NewDocument(&ast.Document{
							Loc:         node.Loc,
							Definitions: definitions,
						})
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}

	editedAst := visitor.Visit(astDoc, v, nil)
	if !reflect.DeepEqual(expectedAST, editedAst) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedAST, editedAst))
	}

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(visited, expectedVisited))
	}
}
func TestVisitor_AllowsForEditingOnEnter(t *testing.T) {

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	expectedQuery := `{ a,    c { a,    c } }`
	expectedAST := parse(t, expectedQuery)
	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				if node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionUpdate, nil
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	editedAst := visitor.Visit(astDoc, v, nil)
	if !reflect.DeepEqual(expectedAST, editedAst) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedAST, editedAst))
	}

}
func TestVisitor_AllowsForEditingOnLeave(t *testing.T) {

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	expectedQuery := `{ a,    c { a,    c } }`
	expectedAST := parse(t, expectedQuery)
	v := &visitor.VisitorOptions{
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				if node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionUpdate, nil
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	editedAst := visitor.Visit(astDoc, v, nil)
	if !reflect.DeepEqual(expectedAST, editedAst) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedAST, editedAst))
	}
}

func TestVisitor_VisitsEditedNode(t *testing.T) {

	query := `{ a { x } }`
	astDoc := parse(t, query)

	addedField := &ast.Field{
		Kind: "Field",
		Name: &ast.Name{
			Kind:  "Name",
			Value: "__typename",
		},
	}

	didVisitAddedField := false
	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				if node.Name != nil && node.Name.Value == "a" {
					s := node.SelectionSet.Selections
					s = append(s, addedField)
					ss := node.SelectionSet
					ss.Selections = s
					return visitor.ActionUpdate, ast.NewField(&ast.Field{
						Kind:         "Field",
						SelectionSet: ss,
					})
				}
				if reflect.DeepEqual(node, addedField) {
					didVisitAddedField = true
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, v, nil)
	if didVisitAddedField == false {
		t.Fatalf("Unexpected result, expected didVisitAddedField == true")
	}
}
func TestVisitor_AllowsSkippingASubTree(t *testing.T) {

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	visited := []interface{}{}
	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "OperationDefinition", nil},
		[]interface{}{"leave", "Document", nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case *ast.Field:
				visited = append(visited, []interface{}{"enter", node.Kind, nil})
				if node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionSkip, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, v, nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_AllowsEarlyExitWhileVisiting(t *testing.T) {

	visited := []interface{}{}

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "x"},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
				if node.Value == "x" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, v, nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_AllowsEarlyExitWhileLeaving(t *testing.T) {

	visited := []interface{}{}

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "x"},
		[]interface{}{"leave", "Name", "x"},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
				if node.Value == "x" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, v, nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_AllowsANamedFunctionsVisitorAPI(t *testing.T) {

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	visited := []interface{}{}
	expectedVisited := []interface{}{
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Name", "x"},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "SelectionSet", nil},
	}

	v := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			"Name": {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					switch node := p.Node.(type) {
					case *ast.Name:
						visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
					}
					return visitor.ActionNoChange, nil
				},
			},
			"SelectionSet": {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					switch node := p.Node.(type) {
					case *ast.SelectionSet:
						visited = append(visited, []interface{}{"enter", node.Kind, nil})
					}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					switch node := p.Node.(type) {
					case *ast.SelectionSet:
						visited = append(visited, []interface{}{"leave", node.Kind, nil})
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}

	_ = visitor.Visit(astDoc, v, nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}
func TestVisitor_VisitsKitchenSink(t *testing.T) {
	b, err := ioutil.ReadFile("../../kitchen-sink.graphql")
	if err != nil {
		t.Fatalf("unable to load kitchen-sink.graphql")
	}

	query := string(b)
	astDoc := parse(t, query)

	visited := []interface{}{}
	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil, nil},
		[]interface{}{"enter", "OperationDefinition", 0, nil},
		[]interface{}{"enter", "Name", "Name", "OperationDefinition"},
		[]interface{}{"leave", "Name", "Name", "OperationDefinition"},
		[]interface{}{"enter", "VariableDefinition", 0, nil},
		[]interface{}{"enter", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Named", "Type", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Named"},
		[]interface{}{"leave", "Name", "Name", "Named"},
		[]interface{}{"leave", "Named", "Type", "VariableDefinition"},
		[]interface{}{"leave", "VariableDefinition", 0, nil},
		[]interface{}{"enter", "VariableDefinition", 1, nil},
		[]interface{}{"enter", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Named", "Type", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Named"},
		[]interface{}{"leave", "Name", "Name", "Named"},
		[]interface{}{"leave", "Named", "Type", "VariableDefinition"},
		[]interface{}{"enter", "EnumValue", "DefaultValue", "VariableDefinition"},
		[]interface{}{"leave", "EnumValue", "DefaultValue", "VariableDefinition"},
		[]interface{}{"leave", "VariableDefinition", 1, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Alias", "Field"},
		[]interface{}{"leave", "Name", "Alias", "Field"},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "ListValue", "Value", "Argument"},
		[]interface{}{"enter", "IntValue", 0, nil},
		[]interface{}{"leave", "IntValue", 0, nil},
		[]interface{}{"enter", "IntValue", 1, nil},
		[]interface{}{"leave", "IntValue", 1, nil},
		[]interface{}{"leave", "ListValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"enter", "InlineFragment", 1, nil},
		[]interface{}{"enter", "Named", "TypeCondition", "InlineFragment"},
		[]interface{}{"enter", "Name", "Name", "Named"},
		[]interface{}{"leave", "Name", "Name", "Named"},
		[]interface{}{"leave", "Named", "TypeCondition", "InlineFragment"},
		[]interface{}{"enter", "Directive", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Directive", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "InlineFragment"},

		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"enter", "Field", 1, nil},
		[]interface{}{"enter", "Name", "Alias", "Field"},
		[]interface{}{"leave", "Name", "Alias", "Field"},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "IntValue", "Value", "Argument"},
		[]interface{}{"leave", "IntValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "Argument", 1, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 1, nil},
		[]interface{}{"enter", "Directive", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Name", "Name", "Directive"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"leave", "Directive", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"enter", "FragmentSpread", 1, nil},
		[]interface{}{"enter", "Name", "Name", "FragmentSpread"},
		[]interface{}{"leave", "Name", "Name", "FragmentSpread"},
		[]interface{}{"leave", "FragmentSpread", 1, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 1, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "InlineFragment"},
		[]interface{}{"leave", "InlineFragment", 1, nil},
		[]interface{}{"enter", "InlineFragment", 2, nil},
		[]interface{}{"enter", "Directive", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Name", "Name", "Directive"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},

		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"leave", "Directive", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "InlineFragment"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "InlineFragment"},
		[]interface{}{"leave", "InlineFragment", 2, nil},
		[]interface{}{"enter", "InlineFragment", 3, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "InlineFragment"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "InlineFragment"},
		[]interface{}{"leave", "InlineFragment", 3, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"leave", "OperationDefinition", 0, nil},
		[]interface{}{"enter", "OperationDefinition", 1, nil},
		[]interface{}{"enter", "Name", "Name", "OperationDefinition"},
		[]interface{}{"leave", "Name", "Name", "OperationDefinition"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "IntValue", "Value", "Argument"},
		[]interface{}{"leave", "IntValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "Directive", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Name", "Name", "Directive"},
		[]interface{}{"leave", "Directive", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"leave", "OperationDefinition", 1, nil},
		[]interface{}{"enter", "OperationDefinition", 2, nil},
		[]interface{}{"enter", "Name", "Name", "OperationDefinition"},
		[]interface{}{"leave", "Name", "Name", "OperationDefinition"},
		[]interface{}{"enter", "VariableDefinition", 0, nil},
		[]interface{}{"enter", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},

		[]interface{}{"leave", "Variable", "Variable", "VariableDefinition"},
		[]interface{}{"enter", "Named", "Type", "VariableDefinition"},
		[]interface{}{"enter", "Name", "Name", "Named"},
		[]interface{}{"leave", "Name", "Name", "Named"},
		[]interface{}{"leave", "Named", "Type", "VariableDefinition"},
		[]interface{}{"leave", "VariableDefinition", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"enter", "Field", 1, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 1, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "Field"},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"leave", "OperationDefinition", 2, nil},
		[]interface{}{"enter", "FragmentDefinition", 3, nil},
		[]interface{}{"enter", "Name", "Name", "FragmentDefinition"},
		[]interface{}{"leave", "Name", "Name", "FragmentDefinition"},
		[]interface{}{"enter", "Named", "TypeCondition", "FragmentDefinition"},
		[]interface{}{"enter", "Name", "Name", "Named"},
		[]interface{}{"leave", "Name", "Name", "Named"},
		[]interface{}{"leave", "Named", "TypeCondition", "FragmentDefinition"},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "FragmentDefinition"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},

		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "Argument", 1, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "Variable", "Value", "Argument"},
		[]interface{}{"enter", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Name", "Name", "Variable"},
		[]interface{}{"leave", "Variable", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 1, nil},
		[]interface{}{"enter", "Argument", 2, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "ObjectValue", "Value", "Argument"},
		[]interface{}{"enter", "ObjectField", 0, nil},
		[]interface{}{"enter", "Name", "Name", "ObjectField"},
		[]interface{}{"leave", "Name", "Name", "ObjectField"},
		[]interface{}{"enter", "StringValue", "Value", "ObjectField"},
		[]interface{}{"leave", "StringValue", "Value", "ObjectField"},
		[]interface{}{"leave", "ObjectField", 0, nil},
		[]interface{}{"leave", "ObjectValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 2, nil},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "FragmentDefinition"},
		[]interface{}{"leave", "FragmentDefinition", 3, nil},
		[]interface{}{"enter", "OperationDefinition", 4, nil},
		[]interface{}{"enter", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"enter", "Field", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"enter", "Argument", 0, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "BooleanValue", "Value", "Argument"},
		[]interface{}{"leave", "BooleanValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 0, nil},
		[]interface{}{"enter", "Argument", 1, nil},
		[]interface{}{"enter", "Name", "Name", "Argument"},
		[]interface{}{"leave", "Name", "Name", "Argument"},
		[]interface{}{"enter", "BooleanValue", "Value", "Argument"},
		[]interface{}{"leave", "BooleanValue", "Value", "Argument"},
		[]interface{}{"leave", "Argument", 1, nil},
		[]interface{}{"leave", "Field", 0, nil},
		[]interface{}{"enter", "Field", 1, nil},
		[]interface{}{"enter", "Name", "Name", "Field"},
		[]interface{}{"leave", "Name", "Name", "Field"},
		[]interface{}{"leave", "Field", 1, nil},
		[]interface{}{"leave", "SelectionSet", "SelectionSet", "OperationDefinition"},
		[]interface{}{"leave", "OperationDefinition", 4, nil},
		[]interface{}{"leave", "Document", nil, nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case ast.Node:
				if p.Parent != nil {
					visited = append(visited, []interface{}{"enter", node.GetKind(), p.Key, p.Parent.GetKind()})
				} else {
					visited = append(visited, []interface{}{"enter", node.GetKind(), p.Key, nil})
				}
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case ast.Node:
				if p.Parent != nil {
					visited = append(visited, []interface{}{"leave", node.GetKind(), p.Key, p.Parent.GetKind()})
				} else {
					visited = append(visited, []interface{}{"leave", node.GetKind(), p.Key, nil})
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, v, nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsSkippingASubTree(t *testing.T) {

	// Note: nearly identical to the above test of the same test but
	// using visitInParallel.

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	visited := []interface{}{}
	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "OperationDefinition", nil},
		[]interface{}{"leave", "Document", nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case *ast.Field:
				visited = append(visited, []interface{}{"enter", node.Kind, nil})
				if node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionSkip, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsSkippingDifferentSubTrees(t *testing.T) {

	query := `{ a { x }, b { y} }`
	astDoc := parse(t, query)

	visited := []interface{}{}
	expectedVisited := []interface{}{
		[]interface{}{"no-a", "enter", "Document", nil},
		[]interface{}{"no-b", "enter", "Document", nil},
		[]interface{}{"no-a", "enter", "OperationDefinition", nil},
		[]interface{}{"no-b", "enter", "OperationDefinition", nil},
		[]interface{}{"no-a", "enter", "SelectionSet", nil},
		[]interface{}{"no-b", "enter", "SelectionSet", nil},
		[]interface{}{"no-a", "enter", "Field", nil},
		[]interface{}{"no-b", "enter", "Field", nil},
		[]interface{}{"no-b", "enter", "Name", "a"},
		[]interface{}{"no-b", "leave", "Name", "a"},
		[]interface{}{"no-b", "enter", "SelectionSet", nil},
		[]interface{}{"no-b", "enter", "Field", nil},
		[]interface{}{"no-b", "enter", "Name", "x"},
		[]interface{}{"no-b", "leave", "Name", "x"},
		[]interface{}{"no-b", "leave", "Field", nil},
		[]interface{}{"no-b", "leave", "SelectionSet", nil},
		[]interface{}{"no-b", "leave", "Field", nil},
		[]interface{}{"no-a", "enter", "Field", nil},
		[]interface{}{"no-b", "enter", "Field", nil},
		[]interface{}{"no-a", "enter", "Name", "b"},
		[]interface{}{"no-a", "leave", "Name", "b"},
		[]interface{}{"no-a", "enter", "SelectionSet", nil},
		[]interface{}{"no-a", "enter", "Field", nil},
		[]interface{}{"no-a", "enter", "Name", "y"},
		[]interface{}{"no-a", "leave", "Name", "y"},
		[]interface{}{"no-a", "leave", "Field", nil},
		[]interface{}{"no-a", "leave", "SelectionSet", nil},
		[]interface{}{"no-a", "leave", "Field", nil},
		[]interface{}{"no-a", "leave", "SelectionSet", nil},
		[]interface{}{"no-b", "leave", "SelectionSet", nil},
		[]interface{}{"no-a", "leave", "OperationDefinition", nil},
		[]interface{}{"no-b", "leave", "OperationDefinition", nil},
		[]interface{}{"no-a", "leave", "Document", nil},
		[]interface{}{"no-b", "leave", "Document", nil},
	}

	v := []*visitor.VisitorOptions{
		{
			Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
				switch node := p.Node.(type) {
				case *ast.Name:
					visited = append(visited, []interface{}{"no-a", "enter", node.Kind, node.Value})
				case *ast.Field:
					visited = append(visited, []interface{}{"no-a", "enter", node.Kind, nil})
					if node.Name != nil && node.Name.Value == "a" {
						return visitor.ActionSkip, nil
					}
				case ast.Node:
					visited = append(visited, []interface{}{"no-a", "enter", node.GetKind(), nil})
				default:
					visited = append(visited, []interface{}{"no-a", "enter", nil, nil})
				}
				return visitor.ActionNoChange, nil
			},
			Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
				switch node := p.Node.(type) {
				case *ast.Name:
					visited = append(visited, []interface{}{"no-a", "leave", node.Kind, node.Value})
				case ast.Node:
					visited = append(visited, []interface{}{"no-a", "leave", node.GetKind(), nil})
				default:
					visited = append(visited, []interface{}{"no-a", "leave", nil, nil})
				}
				return visitor.ActionNoChange, nil
			},
		},
		{
			Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
				switch node := p.Node.(type) {
				case *ast.Name:
					visited = append(visited, []interface{}{"no-b", "enter", node.Kind, node.Value})
				case *ast.Field:
					visited = append(visited, []interface{}{"no-b", "enter", node.Kind, nil})
					if node.Name != nil && node.Name.Value == "b" {
						return visitor.ActionSkip, nil
					}
				case ast.Node:
					visited = append(visited, []interface{}{"no-b", "enter", node.GetKind(), nil})
				default:
					visited = append(visited, []interface{}{"no-b", "enter", nil, nil})
				}
				return visitor.ActionNoChange, nil
			},
			Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
				switch node := p.Node.(type) {
				case *ast.Name:
					visited = append(visited, []interface{}{"no-b", "leave", node.Kind, node.Value})
				case ast.Node:
					visited = append(visited, []interface{}{"no-b", "leave", node.GetKind(), nil})
				default:
					visited = append(visited, []interface{}{"no-b", "leave", nil, nil})
				}
				return visitor.ActionNoChange, nil
			},
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v...), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsEarlyExitWhileVisiting(t *testing.T) {

	// Note: nearly identical to the above test of the same test but
	// using visitInParallel.

	visited := []interface{}{}

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "x"},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
				if node.Value == "x" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsEarlyExitFromDifferentPoints(t *testing.T) {

	visited := []interface{}{}

	query := `{ a { y }, b { x } }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"break-a", "enter", "Document", nil},
		[]interface{}{"break-b", "enter", "Document", nil},
		[]interface{}{"break-a", "enter", "OperationDefinition", nil},
		[]interface{}{"break-b", "enter", "OperationDefinition", nil},
		[]interface{}{"break-a", "enter", "SelectionSet", nil},
		[]interface{}{"break-b", "enter", "SelectionSet", nil},
		[]interface{}{"break-a", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-a", "enter", "Name", "a"},
		[]interface{}{"break-b", "enter", "Name", "a"},
		[]interface{}{"break-b", "leave", "Name", "a"},
		[]interface{}{"break-b", "enter", "SelectionSet", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Name", "y"},
		[]interface{}{"break-b", "leave", "Name", "y"},
		[]interface{}{"break-b", "leave", "Field", nil},
		[]interface{}{"break-b", "leave", "SelectionSet", nil},
		[]interface{}{"break-b", "leave", "Field", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Name", "b"},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-a", "enter", node.Kind, node.Value})
				if node != nil && node.Value == "a" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"break-a", "enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-a", "enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-a", "leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-a", "leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-a", "leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	v2 := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-b", "enter", node.Kind, node.Value})
				if node != nil && node.Value == "b" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"break-b", "enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-b", "enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-b", "leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-b", "leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-b", "leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v, v2), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsEarlyExitWhileLeaving(t *testing.T) {

	visited := []interface{}{}

	query := `{ a, b { x }, c }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "x"},
		[]interface{}{"leave", "Name", "x"},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
				if node.Value == "x" {
					return visitor.ActionBreak, nil
				}
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsEarlyExitFromLeavingDifferentPoints(t *testing.T) {

	visited := []interface{}{}

	query := `{ a { y }, b { x } }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"break-a", "enter", "Document", nil},
		[]interface{}{"break-b", "enter", "Document", nil},
		[]interface{}{"break-a", "enter", "OperationDefinition", nil},
		[]interface{}{"break-b", "enter", "OperationDefinition", nil},
		[]interface{}{"break-a", "enter", "SelectionSet", nil},
		[]interface{}{"break-b", "enter", "SelectionSet", nil},
		[]interface{}{"break-a", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-a", "enter", "Name", "a"},
		[]interface{}{"break-b", "enter", "Name", "a"},
		[]interface{}{"break-a", "leave", "Name", "a"},
		[]interface{}{"break-b", "leave", "Name", "a"},
		[]interface{}{"break-a", "enter", "SelectionSet", nil},
		[]interface{}{"break-b", "enter", "SelectionSet", nil},
		[]interface{}{"break-a", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-a", "enter", "Name", "y"},
		[]interface{}{"break-b", "enter", "Name", "y"},
		[]interface{}{"break-a", "leave", "Name", "y"},
		[]interface{}{"break-b", "leave", "Name", "y"},
		[]interface{}{"break-a", "leave", "Field", nil},
		[]interface{}{"break-b", "leave", "Field", nil},
		[]interface{}{"break-a", "leave", "SelectionSet", nil},
		[]interface{}{"break-b", "leave", "SelectionSet", nil},
		[]interface{}{"break-a", "leave", "Field", nil},
		[]interface{}{"break-b", "leave", "Field", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Name", "b"},
		[]interface{}{"break-b", "leave", "Name", "b"},
		[]interface{}{"break-b", "enter", "SelectionSet", nil},
		[]interface{}{"break-b", "enter", "Field", nil},
		[]interface{}{"break-b", "enter", "Name", "x"},
		[]interface{}{"break-b", "leave", "Name", "x"},
		[]interface{}{"break-b", "leave", "Field", nil},
		[]interface{}{"break-b", "leave", "SelectionSet", nil},
		[]interface{}{"break-b", "leave", "Field", nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-a", "enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-a", "enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-a", "enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				visited = append(visited, []interface{}{"break-a", "leave", node.GetKind(), nil})
				if node.Name != nil && node.Name.Value == "a" {
					return visitor.ActionBreak, nil
				}
			case *ast.Name:
				visited = append(visited, []interface{}{"break-a", "leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-a", "leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-a", "leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	v2 := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"break-b", "enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-b", "enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-b", "enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				visited = append(visited, []interface{}{"break-b", "leave", node.GetKind(), nil})
				if node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionBreak, nil
				}
			case *ast.Name:
				visited = append(visited, []interface{}{"break-b", "leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"break-b", "leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"break-b", "leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v, v2), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsForEditingOnEnter(t *testing.T) {

	visited := []interface{}{}

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "OperationDefinition", nil},
		[]interface{}{"leave", "Document", nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				if node != nil && node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionUpdate, nil
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	v2 := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitInParallel(v, v2), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}
}

func TestVisitor_VisitInParallel_AllowsForEditingOnLeave(t *testing.T) {

	visited := []interface{}{}

	query := `{ a, b, c { a, b, c } }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil},
		[]interface{}{"enter", "OperationDefinition", nil},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"enter", "SelectionSet", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "a"},
		[]interface{}{"leave", "Name", "a"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "b"},
		[]interface{}{"leave", "Name", "b"},
		[]interface{}{"enter", "Field", nil},
		[]interface{}{"enter", "Name", "c"},
		[]interface{}{"leave", "Name", "c"},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "Field", nil},
		[]interface{}{"leave", "SelectionSet", nil},
		[]interface{}{"leave", "OperationDefinition", nil},
		[]interface{}{"leave", "Document", nil},
	}

	v := &visitor.VisitorOptions{
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Field:
				if node != nil && node.Name != nil && node.Name.Value == "b" {
					return visitor.ActionUpdate, nil
				}
			}
			return visitor.ActionNoChange, nil
		},
	}

	v2 := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil})
			}
			return visitor.ActionNoChange, nil
		},
	}

	editedAST := visitor.Visit(astDoc, visitor.VisitInParallel(v, v2), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}

	expectedEditedAST := parse(t, `{ a,    c { a,    c } }`)
	if !reflect.DeepEqual(editedAST, expectedEditedAST) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(editedAST, expectedEditedAST))
	}
}

func TestVisitor_VisitWithTypeInfo_MaintainsTypeInfoDuringVisit(t *testing.T) {

	visited := []interface{}{}

	typeInfo := graphql.NewTypeInfo(&graphql.TypeInfoConfig{
		Schema: testutil.TestSchema,
	})

	query := `{ human(id: 4) { name, pets { name }, unknown } }`
	astDoc := parse(t, query)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil, nil, nil, nil},
		[]interface{}{"enter", "OperationDefinition", nil, nil, "QueryRoot", nil},
		[]interface{}{"enter", "SelectionSet", nil, "QueryRoot", "QueryRoot", nil},
		[]interface{}{"enter", "Field", nil, "QueryRoot", "Human", nil},
		[]interface{}{"enter", "Name", "human", "QueryRoot", "Human", nil},
		[]interface{}{"leave", "Name", "human", "QueryRoot", "Human", nil},
		[]interface{}{"enter", "Argument", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "Name", "id", "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "Name", "id", "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "IntValue", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "IntValue", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "Argument", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "SelectionSet", nil, "Human", "Human", nil},
		[]interface{}{"enter", "Field", nil, "Human", "String", nil},
		[]interface{}{"enter", "Name", "name", "Human", "String", nil},
		[]interface{}{"leave", "Name", "name", "Human", "String", nil},
		[]interface{}{"leave", "Field", nil, "Human", "String", nil},
		[]interface{}{"enter", "Field", nil, "Human", "[Pet]", nil},
		[]interface{}{"enter", "Name", "pets", "Human", "[Pet]", nil},
		[]interface{}{"leave", "Name", "pets", "Human", "[Pet]", nil},
		[]interface{}{"enter", "SelectionSet", nil, "Pet", "[Pet]", nil},
		[]interface{}{"enter", "Field", nil, "Pet", "String", nil},
		[]interface{}{"enter", "Name", "name", "Pet", "String", nil},
		[]interface{}{"leave", "Name", "name", "Pet", "String", nil},
		[]interface{}{"leave", "Field", nil, "Pet", "String", nil},
		[]interface{}{"leave", "SelectionSet", nil, "Pet", "[Pet]", nil},
		[]interface{}{"leave", "Field", nil, "Human", "[Pet]", nil},
		[]interface{}{"enter", "Field", nil, "Human", nil, nil},
		[]interface{}{"enter", "Name", "unknown", "Human", nil, nil},
		[]interface{}{"leave", "Name", "unknown", "Human", nil, nil},
		[]interface{}{"leave", "Field", nil, "Human", nil, nil},
		[]interface{}{"leave", "SelectionSet", nil, "Human", "Human", nil},
		[]interface{}{"leave", "Field", nil, "QueryRoot", "Human", nil},
		[]interface{}{"leave", "SelectionSet", nil, "QueryRoot", "QueryRoot", nil},
		[]interface{}{"leave", "OperationDefinition", nil, nil, "QueryRoot", nil},
		[]interface{}{"leave", "Document", nil, nil, nil, nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			var parentType interface{}
			var ttype interface{}
			var inputType interface{}

			if typeInfo.ParentType() != nil {
				parentType = fmt.Sprintf("%v", typeInfo.ParentType())
			}
			if typeInfo.Type() != nil {
				ttype = fmt.Sprintf("%v", typeInfo.Type())
			}
			if typeInfo.InputType() != nil {
				inputType = fmt.Sprintf("%v", typeInfo.InputType())
			}

			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value, parentType, ttype, inputType})
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil, parentType, ttype, inputType})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil, parentType, ttype, inputType})
			}
			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			var parentType interface{}
			var ttype interface{}
			var inputType interface{}

			if typeInfo.ParentType() != nil {
				parentType = fmt.Sprintf("%v", typeInfo.ParentType())
			}
			if typeInfo.Type() != nil {
				ttype = fmt.Sprintf("%v", typeInfo.Type())
			}
			if typeInfo.InputType() != nil {
				inputType = fmt.Sprintf("%v", typeInfo.InputType())
			}

			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value, parentType, ttype, inputType})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil, parentType, ttype, inputType})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil, parentType, ttype, inputType})
			}
			return visitor.ActionNoChange, nil
		},
	}

	_ = visitor.Visit(astDoc, visitor.VisitWithTypeInfo(typeInfo, v), nil)

	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}

}

func TestVisitor_VisitWithTypeInfo_MaintainsTypeInfoDuringEdit(t *testing.T) {

	visited := []interface{}{}

	typeInfo := graphql.NewTypeInfo(&graphql.TypeInfoConfig{
		Schema: testutil.TestSchema,
	})

	astDoc := parse(t, `{ human(id: 4) { name, pets }, alien }`)

	expectedVisited := []interface{}{
		[]interface{}{"enter", "Document", nil, nil, nil, nil},
		[]interface{}{"enter", "OperationDefinition", nil, nil, "QueryRoot", nil},
		[]interface{}{"enter", "SelectionSet", nil, "QueryRoot", "QueryRoot", nil},
		[]interface{}{"enter", "Field", nil, "QueryRoot", "Human", nil},
		[]interface{}{"enter", "Name", "human", "QueryRoot", "Human", nil},
		[]interface{}{"leave", "Name", "human", "QueryRoot", "Human", nil},
		[]interface{}{"enter", "Argument", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "Name", "id", "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "Name", "id", "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "IntValue", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "IntValue", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"leave", "Argument", nil, "QueryRoot", "Human", "ID"},
		[]interface{}{"enter", "SelectionSet", nil, "Human", "Human", nil},
		[]interface{}{"enter", "Field", nil, "Human", "String", nil},
		[]interface{}{"enter", "Name", "name", "Human", "String", nil},
		[]interface{}{"leave", "Name", "name", "Human", "String", nil},
		[]interface{}{"leave", "Field", nil, "Human", "String", nil},
		[]interface{}{"enter", "Field", nil, "Human", "[Pet]", nil},
		[]interface{}{"enter", "Name", "pets", "Human", "[Pet]", nil},
		[]interface{}{"leave", "Name", "pets", "Human", "[Pet]", nil},
		[]interface{}{"enter", "SelectionSet", nil, "Pet", "[Pet]", nil},
		[]interface{}{"enter", "Field", nil, "Pet", "String!", nil},
		[]interface{}{"enter", "Name", "__typename", "Pet", "String!", nil},
		[]interface{}{"leave", "Name", "__typename", "Pet", "String!", nil},
		[]interface{}{"leave", "Field", nil, "Pet", "String!", nil},
		[]interface{}{"leave", "SelectionSet", nil, "Pet", "[Pet]", nil},
		[]interface{}{"leave", "Field", nil, "Human", "[Pet]", nil},
		[]interface{}{"leave", "SelectionSet", nil, "Human", "Human", nil},
		[]interface{}{"leave", "Field", nil, "QueryRoot", "Human", nil},
		[]interface{}{"enter", "Field", nil, "QueryRoot", "Alien", nil},
		[]interface{}{"enter", "Name", "alien", "QueryRoot", "Alien", nil},
		[]interface{}{"leave", "Name", "alien", "QueryRoot", "Alien", nil},
		[]interface{}{"enter", "SelectionSet", nil, "Alien", "Alien", nil},
		[]interface{}{"enter", "Field", nil, "Alien", "String!", nil},
		[]interface{}{"enter", "Name", "__typename", "Alien", "String!", nil},
		[]interface{}{"leave", "Name", "__typename", "Alien", "String!", nil},
		[]interface{}{"leave", "Field", nil, "Alien", "String!", nil},
		[]interface{}{"leave", "SelectionSet", nil, "Alien", "Alien", nil},
		[]interface{}{"leave", "Field", nil, "QueryRoot", "Alien", nil},
		[]interface{}{"leave", "SelectionSet", nil, "QueryRoot", "QueryRoot", nil},
		[]interface{}{"leave", "OperationDefinition", nil, nil, "QueryRoot", nil},
		[]interface{}{"leave", "Document", nil, nil, nil, nil},
	}

	v := &visitor.VisitorOptions{
		Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
			var parentType interface{}
			var ttype interface{}
			var inputType interface{}

			if typeInfo.ParentType() != nil {
				parentType = fmt.Sprintf("%v", typeInfo.ParentType())
			}
			if typeInfo.Type() != nil {
				ttype = fmt.Sprintf("%v", typeInfo.Type())
			}
			if typeInfo.InputType() != nil {
				inputType = fmt.Sprintf("%v", typeInfo.InputType())
			}

			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"enter", node.Kind, node.Value, parentType, ttype, inputType})
			case *ast.Field:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil, parentType, ttype, inputType})

				// Make a query valid by adding missing selection sets.
				if node.SelectionSet == nil && graphql.IsCompositeType(graphql.GetNamed(typeInfo.Type())) {
					return visitor.ActionUpdate, ast.NewField(&ast.Field{
						Alias:      node.Alias,
						Name:       node.Name,
						Arguments:  node.Arguments,
						Directives: node.Directives,
						SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
							Selections: []ast.Selection{
								ast.NewField(&ast.Field{
									Name: ast.NewName(&ast.Name{
										Value: "__typename",
									}),
								}),
							},
						}),
					})
				}
			case ast.Node:
				visited = append(visited, []interface{}{"enter", node.GetKind(), nil, parentType, ttype, inputType})
			default:
				visited = append(visited, []interface{}{"enter", nil, nil, parentType, ttype, inputType})
			}

			return visitor.ActionNoChange, nil
		},
		Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
			var parentType interface{}
			var ttype interface{}
			var inputType interface{}

			if typeInfo.ParentType() != nil {
				parentType = fmt.Sprintf("%v", typeInfo.ParentType())
			}
			if typeInfo.Type() != nil {
				ttype = fmt.Sprintf("%v", typeInfo.Type())
			}
			if typeInfo.InputType() != nil {
				inputType = fmt.Sprintf("%v", typeInfo.InputType())
			}

			switch node := p.Node.(type) {
			case *ast.Name:
				visited = append(visited, []interface{}{"leave", node.Kind, node.Value, parentType, ttype, inputType})
			case ast.Node:
				visited = append(visited, []interface{}{"leave", node.GetKind(), nil, parentType, ttype, inputType})
			default:
				visited = append(visited, []interface{}{"leave", nil, nil, parentType, ttype, inputType})
			}
			return visitor.ActionNoChange, nil
		},
	}

	editedAST := visitor.Visit(astDoc, visitor.VisitWithTypeInfo(typeInfo, v), nil)

	editedASTQuery := printer.Print(editedAST.(ast.Node))
	expectedEditedASTQuery := printer.Print(parse(t, `{ human(id: 4) { name, pets { __typename } }, alien { __typename } }`))

	if !reflect.DeepEqual(editedASTQuery, expectedEditedASTQuery) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(editedASTQuery, expectedEditedASTQuery))
	}
	if !reflect.DeepEqual(visited, expectedVisited) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expectedVisited, visited))
	}

}
