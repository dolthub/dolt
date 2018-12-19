package visitor

import (
	"encoding/json"
	"fmt"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/typeInfo"
	"reflect"
)

const (
	ActionNoChange = ""
	ActionBreak    = "BREAK"
	ActionSkip     = "SKIP"
	ActionUpdate   = "UPDATE"
)

type KeyMap map[string][]string

// note that the keys are in Capital letters, equivalent to the ast.Node field Names
var QueryDocumentKeys = KeyMap{
	"Name":     []string{},
	"Document": []string{"Definitions"},
	"OperationDefinition": []string{
		"Name",
		"VariableDefinitions",
		"Directives",
		"SelectionSet",
	},
	"VariableDefinition": []string{
		"Variable",
		"Type",
		"DefaultValue",
	},
	"Variable":     []string{"Name"},
	"SelectionSet": []string{"Selections"},
	"Field": []string{
		"Alias",
		"Name",
		"Arguments",
		"Directives",
		"SelectionSet",
	},
	"Argument": []string{
		"Name",
		"Value",
	},

	"FragmentSpread": []string{
		"Name",
		"Directives",
	},
	"InlineFragment": []string{
		"TypeCondition",
		"Directives",
		"SelectionSet",
	},
	"FragmentDefinition": []string{
		"Name",
		"TypeCondition",
		"Directives",
		"SelectionSet",
	},

	"IntValue":     []string{},
	"FloatValue":   []string{},
	"StringValue":  []string{},
	"BooleanValue": []string{},
	"EnumValue":    []string{},
	"ListValue":    []string{"Values"},
	"ObjectValue":  []string{"Fields"},
	"ObjectField": []string{
		"Name",
		"Value",
	},

	"Directive": []string{
		"Name",
		"Arguments",
	},

	"Named":   []string{"Name"},
	"List":    []string{"Type"},
	"NonNull": []string{"Type"},

	"SchemaDefinition":        []string{"OperationTypes"},
	"OperationTypeDefinition": []string{"Type"},

	"ScalarDefinition": []string{"Name"},
	"ObjectDefinition": []string{
		"Name",
		"Interfaces",
		"Fields",
	},
	"FieldDefinition": []string{
		"Name",
		"Arguments",
		"Type",
	},
	"InputValueDefinition": []string{
		"Name",
		"Type",
		"DefaultValue",
	},
	"InterfaceDefinition": []string{
		"Name",
		"Fields",
	},
	"UnionDefinition": []string{
		"Name",
		"Types",
	},
	"EnumDefinition": []string{
		"Name",
		"Values",
	},
	"EnumValueDefinition": []string{"Name"},
	"InputObjectDefinition": []string{
		"Name",
		"Fields",
	},

	"TypeExtensionDefinition": []string{"Definition"},

	"DirectiveDefinition": []string{"Name", "Arguments", "Locations"},
}

type stack struct {
	Index   int
	Keys    []interface{}
	Edits   []*edit
	inSlice bool
	Prev    *stack
}
type edit struct {
	Key   interface{}
	Value interface{}
}

type VisitFuncParams struct {
	Node      interface{}
	Key       interface{}
	Parent    ast.Node
	Path      []interface{}
	Ancestors []ast.Node
}

type VisitFunc func(p VisitFuncParams) (string, interface{})

type NamedVisitFuncs struct {
	Kind  VisitFunc // 1) Named visitors triggered when entering a node a specific kind.
	Leave VisitFunc // 2) Named visitors that trigger upon entering and leaving a node of
	Enter VisitFunc // 2) Named visitors that trigger upon entering and leaving a node of
}

type VisitorOptions struct {
	KindFuncMap map[string]NamedVisitFuncs
	Enter       VisitFunc // 3) Generic visitors that trigger upon entering and leaving any node.
	Leave       VisitFunc // 3) Generic visitors that trigger upon entering and leaving any node.

	EnterKindMap map[string]VisitFunc // 4) Parallel visitors for entering and leaving nodes of a specific kind
	LeaveKindMap map[string]VisitFunc // 4) Parallel visitors for entering and leaving nodes of a specific kind
}

func Visit(root ast.Node, visitorOpts *VisitorOptions, keyMap KeyMap) interface{} {
	visitorKeys := keyMap
	if visitorKeys == nil {
		visitorKeys = QueryDocumentKeys
	}

	var result interface{}
	var newRoot = root
	var sstack *stack
	var parent interface{}
	var parentSlice []interface{}
	inSlice := false
	prevInSlice := false
	keys := []interface{}{newRoot}
	index := -1
	edits := []*edit{}
	path := []interface{}{}
	ancestors := []interface{}{}
	ancestorsSlice := [][]interface{}{}
Loop:
	for {
		index = index + 1

		isLeaving := (len(keys) == index)
		var key interface{}  // string for structs or int for slices
		var node interface{} // ast.Node or can be anything
		var nodeSlice []interface{}
		isEdited := (isLeaving && len(edits) != 0)

		if isLeaving {
			if !inSlice {
				if len(ancestors) == 0 {
					key = nil
				} else {
					key, path = pop(path)
				}
			} else {
				if len(ancestorsSlice) == 0 {
					key = nil
				} else {
					key, path = pop(path)
				}
			}

			node = parent
			parent, ancestors = pop(ancestors)
			nodeSlice = parentSlice
			parentSlice, ancestorsSlice = popNodeSlice(ancestorsSlice)

			if isEdited {
				prevInSlice = inSlice
				editOffset := 0
				for _, edit := range edits {
					arrayEditKey := 0
					if inSlice {
						keyInt := edit.Key.(int)
						edit.Key = keyInt - editOffset
						arrayEditKey = edit.Key.(int)
					}
					if inSlice && isNilNode(edit.Value) {
						nodeSlice = spliceNode(nodeSlice, arrayEditKey)
						editOffset = editOffset + 1
					} else {
						if inSlice {
							nodeSlice[arrayEditKey] = edit.Value
						} else {
							key, _ := edit.Key.(string)

							var updatedNode interface{}
							if !isSlice(edit.Value) {
								if isStructNode(edit.Value) {
									updatedNode = updateNodeField(node, key, edit.Value)
								} else {
									var todoNode map[string]interface{}
									b, err := json.Marshal(node)
									if err != nil {
										panic(fmt.Sprintf("Invalid root AST Node: %v", root))
									}
									err = json.Unmarshal(b, &todoNode)
									if err != nil {
										panic(fmt.Sprintf("Invalid root AST Node (2): %v", root))
									}
									todoNode[key] = edit.Value
									updatedNode = todoNode
								}
							} else {
								isSliceOfNodes := true

								// check if edit.value slice is ast.nodes
								switch reflect.TypeOf(edit.Value).Kind() {
								case reflect.Slice:
									s := reflect.ValueOf(edit.Value)
									for i := 0; i < s.Len(); i++ {
										elem := s.Index(i)
										if !isStructNode(elem.Interface()) {
											isSliceOfNodes = false
										}
									}
								}

								// is a slice of real nodes
								if isSliceOfNodes {
									// the node we are writing to is an ast.Node
									updatedNode = updateNodeField(node, key, edit.Value)
								} else {
									var todoNode map[string]interface{}
									b, err := json.Marshal(node)
									if err != nil {
										panic(fmt.Sprintf("Invalid root AST Node: %v", root))
									}
									err = json.Unmarshal(b, &todoNode)
									if err != nil {
										panic(fmt.Sprintf("Invalid root AST Node (2): %v", root))
									}
									todoNode[key] = edit.Value
									updatedNode = todoNode
								}

							}
							node = updatedNode
						}
					}
				}
			}
			index = sstack.Index
			keys = sstack.Keys
			edits = sstack.Edits
			inSlice = sstack.inSlice
			sstack = sstack.Prev
		} else {
			// get key
			if !inSlice {
				if !isNilNode(parent) {
					key = getFieldValue(keys, index)
				} else {
					// initial conditions
					key = nil
				}
			} else {
				key = index
			}
			// get node
			if !inSlice {
				if !isNilNode(parent) {
					fieldValue := getFieldValue(parent, key)
					if isNode(fieldValue) {
						node = fieldValue.(ast.Node)
					}
					if isSlice(fieldValue) {
						nodeSlice = toSliceInterfaces(fieldValue)
					}
				} else {
					// initial conditions
					node = newRoot
				}
			} else {
				if len(parentSlice) != 0 {
					fieldValue := getFieldValue(parentSlice, key)
					if isNode(fieldValue) {
						node = fieldValue.(ast.Node)
					}
					if isSlice(fieldValue) {
						nodeSlice = toSliceInterfaces(fieldValue)
					}
				} else {
					// initial conditions
					nodeSlice = []interface{}{}
				}
			}

			if isNilNode(node) && len(nodeSlice) == 0 {
				continue
			}

			if !inSlice {
				if !isNilNode(parent) {
					path = append(path, key)
				}
			} else {
				if len(parentSlice) != 0 {
					path = append(path, key)
				}
			}
		}

		// get result from visitFn for a node if set
		var result interface{}
		resultIsUndefined := true
		if !isNilNode(node) {
			if !isNode(node) { // is node-ish.
				panic(fmt.Sprintf("Invalid AST Node (4): %v", node))
			}

			// Try to pass in current node as ast.Node
			// Note that since user can potentially return a non-ast.Node from visit functions.
			// In that case, we try to unmarshal map[string]interface{} into ast.Node
			var nodeIn interface{}
			if _, ok := node.(map[string]interface{}); ok {
				b, err := json.Marshal(node)
				if err != nil {
					panic(fmt.Sprintf("Invalid root AST Node: %v", root))
				}
				err = json.Unmarshal(b, &nodeIn)
				if err != nil {
					panic(fmt.Sprintf("Invalid root AST Node (2a): %v", root))
				}
			} else {
				nodeIn = node
			}
			parentConcrete, _ := parent.(ast.Node)
			ancestorsConcrete := []ast.Node{}
			for _, ancestor := range ancestors {
				if ancestorConcrete, ok := ancestor.(ast.Node); ok {
					ancestorsConcrete = append(ancestorsConcrete, ancestorConcrete)
				}
			}

			kind := ""
			if node, ok := node.(map[string]interface{}); ok {
				kind, _ = node["Kind"].(string)
			}
			if node, ok := node.(ast.Node); ok {
				kind = node.GetKind()
			}

			visitFn := GetVisitFn(visitorOpts, kind, isLeaving)
			if visitFn != nil {
				p := VisitFuncParams{
					Node:      nodeIn,
					Key:       key,
					Parent:    parentConcrete,
					Path:      path,
					Ancestors: ancestorsConcrete,
				}
				action := ActionUpdate
				action, result = visitFn(p)
				if action == ActionBreak {
					break Loop
				}
				if action == ActionSkip {
					if !isLeaving {
						_, path = pop(path)
						continue
					}
				}
				if action != ActionNoChange {
					resultIsUndefined = false
					edits = append(edits, &edit{
						Key:   key,
						Value: result,
					})
					if !isLeaving {
						if isNode(result) {
							node = result
						} else {
							_, path = pop(path)
							continue
						}
					}
				} else {
					resultIsUndefined = true
				}
			}

		}

		// collect back edits on the way out
		if resultIsUndefined && isEdited {
			if !prevInSlice {
				edits = append(edits, &edit{
					Key:   key,
					Value: node,
				})
			} else {
				edits = append(edits, &edit{
					Key:   key,
					Value: nodeSlice,
				})
			}
		}
		if !isLeaving {

			// add to stack
			prevStack := sstack
			sstack = &stack{
				inSlice: inSlice,
				Index:   index,
				Keys:    keys,
				Edits:   edits,
				Prev:    prevStack,
			}

			// replace keys
			inSlice = false
			if len(nodeSlice) > 0 {
				inSlice = true
			}
			keys = []interface{}{}

			if inSlice {
				// get keys
				for _, m := range nodeSlice {
					keys = append(keys, m)
				}
			} else {
				if !isNilNode(node) {
					if node, ok := node.(ast.Node); ok {
						kind := node.GetKind()
						if n, ok := visitorKeys[kind]; ok {
							for _, m := range n {
								keys = append(keys, m)
							}
						}
					}

				}

			}
			index = -1
			edits = []*edit{}

			ancestors = append(ancestors, parent)
			parent = node
			ancestorsSlice = append(ancestorsSlice, parentSlice)
			parentSlice = nodeSlice

		}

		// loop guard
		if sstack == nil {
			break Loop
		}
	}
	if len(edits) != 0 {
		result = edits[len(edits)-1].Value
	}
	return result
}

func pop(a []interface{}) (x interface{}, aa []interface{}) {
	if len(a) == 0 {
		return x, aa
	}
	x, aa = a[len(a)-1], a[:len(a)-1]
	return x, aa
}
func popNodeSlice(a [][]interface{}) (x []interface{}, aa [][]interface{}) {
	if len(a) == 0 {
		return x, aa
	}
	x, aa = a[len(a)-1], a[:len(a)-1]
	return x, aa
}
func spliceNode(a interface{}, i int) (result []interface{}) {
	if i < 0 {
		return result
	}
	typeOf := reflect.TypeOf(a)
	if typeOf == nil {
		return result
	}
	switch typeOf.Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(a)
		for i := 0; i < s.Len(); i++ {
			elem := s.Index(i)
			elemInterface := elem.Interface()
			result = append(result, elemInterface)
		}
		if i >= s.Len() {
			return result
		}
		return append(result[:i], result[i+1:]...)
	default:
		return result
	}
}

func getFieldValue(obj interface{}, key interface{}) interface{} {
	val := reflect.ValueOf(obj)
	if val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Type().Kind() == reflect.Struct {
		key, ok := key.(string)
		if !ok {
			return nil
		}
		valField := val.FieldByName(key)
		if valField.IsValid() {
			return valField.Interface()
		}
		return nil
	}
	if val.Type().Kind() == reflect.Slice {
		key, ok := key.(int)
		if !ok {
			return nil
		}
		if key >= val.Len() {
			return nil
		}
		valField := val.Index(key)
		if valField.IsValid() {
			return valField.Interface()
		}
		return nil
	}
	if val.Type().Kind() == reflect.Map {
		keyVal := reflect.ValueOf(key)
		valField := val.MapIndex(keyVal)
		if valField.IsValid() {
			return valField.Interface()
		}
		return nil
	}
	return nil
}

func updateNodeField(value interface{}, fieldName string, fieldValue interface{}) (retVal interface{}) {
	retVal = value
	val := reflect.ValueOf(value)

	isPtr := false
	if val.IsValid() && val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
		isPtr = true
	}
	if !val.IsValid() {
		return retVal
	}
	if val.Type().Kind() == reflect.Struct {
		for i := 0; i < val.NumField(); i++ {
			valueField := val.Field(i)
			typeField := val.Type().Field(i)

			// try matching the field name
			if typeField.Name == fieldName {
				fieldValueVal := reflect.ValueOf(fieldValue)
				if valueField.CanSet() {

					if fieldValueVal.IsValid() {
						if valueField.Type().Kind() == fieldValueVal.Type().Kind() {
							if fieldValueVal.Type().Kind() == reflect.Slice {
								newSliceValue := reflect.MakeSlice(reflect.TypeOf(valueField.Interface()), fieldValueVal.Len(), fieldValueVal.Len())
								for i := 0; i < newSliceValue.Len(); i++ {
									dst := newSliceValue.Index(i)
									src := fieldValueVal.Index(i)
									srcValue := reflect.ValueOf(src.Interface())
									if dst.CanSet() {
										dst.Set(srcValue)
									}
								}
								valueField.Set(newSliceValue)

							} else {
								valueField.Set(fieldValueVal)
							}
						}
					} else {
						valueField.Set(reflect.New(valueField.Type()).Elem())
					}
					if isPtr == true {
						retVal = val.Addr().Interface()
						return retVal
					}
					retVal = val.Interface()
					return retVal

				}
			}
		}
	}
	return retVal
}
func toSliceInterfaces(slice interface{}) (result []interface{}) {
	switch reflect.TypeOf(slice).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(slice)
		for i := 0; i < s.Len(); i++ {
			elem := s.Index(i)
			elemInterface := elem.Interface()
			if elem, ok := elemInterface.(ast.Node); ok {
				result = append(result, elem)
			}
		}
		return result
	default:
		return result
	}
}

func isSlice(value interface{}) bool {
	val := reflect.ValueOf(value)
	if val.IsValid() && val.Type().Kind() == reflect.Slice {
		return true
	}
	return false
}
func isNode(node interface{}) bool {
	val := reflect.ValueOf(node)
	if val.IsValid() && val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if !val.IsValid() {
		return false
	}
	if val.Type().Kind() == reflect.Map {
		keyVal := reflect.ValueOf("Kind")
		valField := val.MapIndex(keyVal)
		return valField.IsValid()
	}
	if val.Type().Kind() == reflect.Struct {
		valField := val.FieldByName("Kind")
		return valField.IsValid()
	}
	return false
}
func isStructNode(node interface{}) bool {
	val := reflect.ValueOf(node)
	if val.IsValid() && val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if !val.IsValid() {
		return false
	}
	if val.Type().Kind() == reflect.Struct {
		valField := val.FieldByName("Kind")
		return valField.IsValid()
	}
	return false
}

func isNilNode(node interface{}) bool {
	val := reflect.ValueOf(node)
	if !val.IsValid() {
		return true
	}
	if val.Type().Kind() == reflect.Ptr {
		return val.IsNil()
	}
	if val.Type().Kind() == reflect.Slice {
		return val.Len() == 0
	}
	if val.Type().Kind() == reflect.Map {
		return val.Len() == 0
	}
	if val.Type().Kind() == reflect.Bool {
		return val.Interface().(bool)
	}
	return val.Interface() == nil
}

// VisitInParallel Creates a new visitor instance which delegates to many visitors to run in
// parallel. Each visitor will be visited for each node before moving on.
//
// If a prior visitor edits a node, no following visitors will see that node.
func VisitInParallel(visitorOptsSlice ...*VisitorOptions) *VisitorOptions {
	skipping := map[int]interface{}{}

	return &VisitorOptions{
		Enter: func(p VisitFuncParams) (string, interface{}) {
			for i, visitorOpts := range visitorOptsSlice {
				if _, ok := skipping[i]; !ok {
					switch node := p.Node.(type) {
					case ast.Node:
						kind := node.GetKind()
						fn := GetVisitFn(visitorOpts, kind, false)
						if fn != nil {
							action, result := fn(p)
							if action == ActionSkip {
								skipping[i] = node
							} else if action == ActionBreak {
								skipping[i] = ActionBreak
							} else if action == ActionUpdate {
								return ActionUpdate, result
							}
						}
					}
				}
			}
			return ActionNoChange, nil
		},
		Leave: func(p VisitFuncParams) (string, interface{}) {
			for i, visitorOpts := range visitorOptsSlice {
				skippedNode, ok := skipping[i]
				if !ok {
					switch node := p.Node.(type) {
					case ast.Node:
						kind := node.GetKind()
						fn := GetVisitFn(visitorOpts, kind, true)
						if fn != nil {
							action, result := fn(p)
							if action == ActionBreak {
								skipping[i] = ActionBreak
							} else if action == ActionUpdate {
								return ActionUpdate, result
							}
						}
					}
				} else if skippedNode == p.Node {
					delete(skipping, i)
				}
			}
			return ActionNoChange, nil
		},
	}
}

// VisitWithTypeInfo Creates a new visitor instance which maintains a provided TypeInfo instance
// along with visiting visitor.
func VisitWithTypeInfo(ttypeInfo typeInfo.TypeInfoI, visitorOpts *VisitorOptions) *VisitorOptions {
	return &VisitorOptions{
		Enter: func(p VisitFuncParams) (string, interface{}) {
			if node, ok := p.Node.(ast.Node); ok {
				ttypeInfo.Enter(node)
				fn := GetVisitFn(visitorOpts, node.GetKind(), false)
				if fn != nil {
					action, result := fn(p)
					if action == ActionUpdate {
						ttypeInfo.Leave(node)
						if isNode(result) {
							if result, ok := result.(ast.Node); ok {
								ttypeInfo.Enter(result)
							}
						}
					}
					return action, result
				}
			}
			return ActionNoChange, nil
		},
		Leave: func(p VisitFuncParams) (string, interface{}) {
			action := ActionNoChange
			var result interface{}
			if node, ok := p.Node.(ast.Node); ok {
				fn := GetVisitFn(visitorOpts, node.GetKind(), true)
				if fn != nil {
					action, result = fn(p)
				}
				ttypeInfo.Leave(node)
			}
			return action, result
		},
	}
}

// GetVisitFn Given a visitor instance, if it is leaving or not, and a node kind, return
// the function the visitor runtime should call.
func GetVisitFn(visitorOpts *VisitorOptions, kind string, isLeaving bool) VisitFunc {
	if visitorOpts == nil {
		return nil
	}
	kindVisitor, ok := visitorOpts.KindFuncMap[kind]
	if ok {
		if !isLeaving && kindVisitor.Kind != nil {
			// { Kind() {} }
			return kindVisitor.Kind
		}
		if isLeaving {
			// { Kind: { leave() {} } }
			return kindVisitor.Leave
		}
		// { Kind: { enter() {} } }
		return kindVisitor.Enter

	}
	if isLeaving {
		// { enter() {} }
		specificVisitor := visitorOpts.Leave
		if specificVisitor != nil {
			return specificVisitor
		}
		if specificKindVisitor, ok := visitorOpts.LeaveKindMap[kind]; ok {
			// { leave: { Kind() {} } }
			return specificKindVisitor
		}

	}
	// { leave() {} }
	specificVisitor := visitorOpts.Enter
	if specificVisitor != nil {
		return specificVisitor
	}
	if specificKindVisitor, ok := visitorOpts.EnterKindMap[kind]; ok {
		// { enter: { Kind() {} } }
		return specificKindVisitor
	}
	return nil
}
