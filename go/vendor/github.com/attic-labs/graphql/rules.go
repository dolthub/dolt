package graphql

import (
	"fmt"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/kinds"
	"github.com/attic-labs/graphql/language/printer"
	"github.com/attic-labs/graphql/language/visitor"
	"sort"
	"strings"
)

// SpecifiedRules set includes all validation rules defined by the GraphQL spec.
var SpecifiedRules = []ValidationRuleFn{
	ArgumentsOfCorrectTypeRule,
	DefaultValuesOfCorrectTypeRule,
	// FieldsOnCorrectTypeRule, <-- commented out for Attic
	FragmentsOnCompositeTypesRule,
	KnownArgumentNamesRule,
	KnownDirectivesRule,
	KnownFragmentNamesRule,
	// KnownTypeNamesRule, <-- commented out for Attic
	LoneAnonymousOperationRule,
	NoFragmentCyclesRule,
	NoUndefinedVariablesRule,
	NoUnusedFragmentsRule,
	NoUnusedVariablesRule,
	OverlappingFieldsCanBeMergedRule,
	// PossibleFragmentSpreadsRule, <-- commented out for Attic
	ProvidedNonNullArgumentsRule,
	ScalarLeafsRule,
	UniqueArgumentNamesRule,
	UniqueFragmentNamesRule,
	UniqueInputFieldNamesRule,
	UniqueOperationNamesRule,
	UniqueVariableNamesRule,
	VariablesAreInputTypesRule,
	VariablesInAllowedPositionRule,
}

type ValidationRuleInstance struct {
	VisitorOpts *visitor.VisitorOptions
}
type ValidationRuleFn func(context *ValidationContext) *ValidationRuleInstance

func newValidationError(message string, nodes []ast.Node) *gqlerrors.Error {
	return gqlerrors.NewError(
		message,
		nodes,
		"",
		nil,
		[]int{},
		nil, // TODO: this is interim, until we port "better-error-messages-for-inputs"
	)
}

func reportError(context *ValidationContext, message string, nodes []ast.Node) (string, interface{}) {
	context.ReportError(newValidationError(message, nodes))
	return visitor.ActionNoChange, nil
}

// ArgumentsOfCorrectTypeRule Argument values of correct type
//
// A GraphQL document is only valid if all field argument literal values are
// of the type expected by their position.
func ArgumentsOfCorrectTypeRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Argument: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if argAST, ok := p.Node.(*ast.Argument); ok {
						value := argAST.Value
						argDef := context.Argument()
						if argDef != nil {
							isValid, messages := isValidLiteralValue(argDef.Type, value)
							if !isValid {
								argNameValue := ""
								if argAST.Name != nil {
									argNameValue = argAST.Name.Value
								}

								messagesStr := ""
								if len(messages) > 0 {
									messagesStr = "\n" + strings.Join(messages, "\n")
								}
								reportError(
									context,
									fmt.Sprintf(`Argument "%v" has invalid value %v.%v`,
										argNameValue, printer.Print(value), messagesStr),
									[]ast.Node{value},
								)
							}

						}
					}
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// DefaultValuesOfCorrectTypeRule Variable default values of correct type
//
// A GraphQL document is only valid if all variable default values are of the
// type expected by their definition.
func DefaultValuesOfCorrectTypeRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if varDefAST, ok := p.Node.(*ast.VariableDefinition); ok {
						name := ""
						if varDefAST.Variable != nil && varDefAST.Variable.Name != nil {
							name = varDefAST.Variable.Name.Value
						}
						defaultValue := varDefAST.DefaultValue
						ttype := context.InputType()

						if ttype, ok := ttype.(*NonNull); ok && defaultValue != nil {
							reportError(
								context,
								fmt.Sprintf(`Variable "$%v" of type "%v" is required and will not use the default value. Perhaps you meant to use type "%v".`,
									name, ttype, ttype.OfType),
								[]ast.Node{defaultValue},
							)
						}
						isValid, messages := isValidLiteralValue(ttype, defaultValue)
						if ttype != nil && defaultValue != nil && !isValid {
							messagesStr := ""
							if len(messages) > 0 {
								messagesStr = "\n" + strings.Join(messages, "\n")
							}
							reportError(
								context,
								fmt.Sprintf(`Variable "$%v" has invalid default value: %v.%v`,
									name, printer.Print(defaultValue), messagesStr),
								[]ast.Node{defaultValue},
							)
						}
					}
					return visitor.ActionSkip, nil
				},
			},
			kinds.SelectionSet: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func UndefinedFieldMessage(fieldName string, ttypeName string, suggestedTypes []string) string {

	quoteStrings := func(slice []string) []string {
		quoted := []string{}
		for _, s := range slice {
			quoted = append(quoted, fmt.Sprintf(`"%v"`, s))
		}
		return quoted
	}

	// construct helpful (but long) message
	message := fmt.Sprintf(`Cannot query field "%v" on type "%v".`, fieldName, ttypeName)
	suggestions := strings.Join(quoteStrings(suggestedTypes), ", ")
	const MaxLength = 5
	if len(suggestedTypes) > 0 {
		if len(suggestedTypes) > MaxLength {
			suggestions = strings.Join(quoteStrings(suggestedTypes[0:MaxLength]), ", ") +
				fmt.Sprintf(`, and %v other types`, len(suggestedTypes)-MaxLength)
		}
		message = message + fmt.Sprintf(` However, this field exists on %v.`, suggestions)
		message = message + ` Perhaps you meant to use an inline fragment?`
	}

	return message
}

// FieldsOnCorrectTypeRule Fields on correct type
//
// A GraphQL document is only valid if all fields selected are defined by the
// parent type, or are an allowed meta field such as __typenamme
func FieldsOnCorrectTypeRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Field: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					var action = visitor.ActionNoChange
					var result interface{}
					if node, ok := p.Node.(*ast.Field); ok {
						ttype := context.ParentType()

						if ttype != nil {
							fieldDef := context.FieldDef()
							if fieldDef == nil {
								// This isn't valid. Let's find suggestions, if any.
								suggestedTypes := []string{}

								nodeName := ""
								if node.Name != nil {
									nodeName = node.Name.Value
								}

								if ttype, ok := ttype.(Abstract); ok && IsAbstractType(ttype) {
									siblingInterfaces := getSiblingInterfacesIncludingField(context.Schema(), ttype, nodeName)
									implementations := getImplementationsIncludingField(context.Schema(), ttype, nodeName)
									suggestedMaps := map[string]bool{}
									for _, s := range siblingInterfaces {
										if _, ok := suggestedMaps[s]; !ok {
											suggestedMaps[s] = true
											suggestedTypes = append(suggestedTypes, s)
										}
									}
									for _, s := range implementations {
										if _, ok := suggestedMaps[s]; !ok {
											suggestedMaps[s] = true
											suggestedTypes = append(suggestedTypes, s)
										}
									}
								}

								message := UndefinedFieldMessage(nodeName, ttype.Name(), suggestedTypes)

								reportError(
									context,
									message,
									[]ast.Node{node},
								)
							}
						}
					}
					return action, result
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// Return implementations of `type` that include `fieldName` as a valid field.
func getImplementationsIncludingField(schema *Schema, ttype Abstract, fieldName string) []string {

	result := []string{}
	for _, t := range schema.PossibleTypes(ttype) {
		fields := t.Fields()
		if _, ok := fields[fieldName]; ok {
			result = append(result, fmt.Sprintf(`%v`, t.Name()))
		}
	}

	sort.Strings(result)
	return result
}

// Go through all of the implementations of type, and find other interaces
// that they implement. If those interfaces include `field` as a valid field,
// return them, sorted by how often the implementations include the other
// interface.
func getSiblingInterfacesIncludingField(schema *Schema, ttype Abstract, fieldName string) []string {
	implementingObjects := schema.PossibleTypes(ttype)

	result := []string{}
	suggestedInterfaceSlice := []*suggestedInterface{}

	// stores a map of interface name => index in suggestedInterfaceSlice
	suggestedInterfaceMap := map[string]int{}

	for _, t := range implementingObjects {
		for _, i := range t.Interfaces() {
			if i == nil {
				continue
			}
			fields := i.Fields()
			if _, ok := fields[fieldName]; !ok {
				continue
			}
			index, ok := suggestedInterfaceMap[i.Name()]
			if !ok {
				suggestedInterfaceSlice = append(suggestedInterfaceSlice, &suggestedInterface{
					name:  i.Name(),
					count: 0,
				})
				index = len(suggestedInterfaceSlice) - 1
			}
			if index < len(suggestedInterfaceSlice) {
				s := suggestedInterfaceSlice[index]
				if s.name == i.Name() {
					s.count = s.count + 1
				}
			}
		}
	}
	sort.Sort(suggestedInterfaceSortedSlice(suggestedInterfaceSlice))

	for _, s := range suggestedInterfaceSlice {
		result = append(result, fmt.Sprintf(`%v`, s.name))
	}
	return result

}

type suggestedInterface struct {
	name  string
	count int
}

type suggestedInterfaceSortedSlice []*suggestedInterface

func (s suggestedInterfaceSortedSlice) Len() int {
	return len(s)
}
func (s suggestedInterfaceSortedSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s suggestedInterfaceSortedSlice) Less(i, j int) bool {
	return s[i].count < s[j].count
}

// FragmentsOnCompositeTypesRule Fragments on composite type
//
// Fragments use a type condition to determine if they apply, since fragments
// can only be spread into a composite type (object, interface, or union), the
// type condition must also be a composite type.
func FragmentsOnCompositeTypesRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.InlineFragment: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.InlineFragment); ok {
						ttype := context.Type()
						if node.TypeCondition != nil && ttype != nil && !IsCompositeType(ttype) {
							reportError(
								context,
								fmt.Sprintf(`Fragment cannot condition on non composite type "%v".`, ttype),
								[]ast.Node{node.TypeCondition},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.FragmentDefinition); ok {
						ttype := context.Type()
						if ttype != nil && !IsCompositeType(ttype) {
							nodeName := ""
							if node.Name != nil {
								nodeName = node.Name.Value
							}
							reportError(
								context,
								fmt.Sprintf(`Fragment "%v" cannot condition on non composite type "%v".`, nodeName, printer.Print(node.TypeCondition)),
								[]ast.Node{node.TypeCondition},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// KnownArgumentNamesRule Known argument names
//
// A GraphQL field is only valid if all supplied arguments are defined by
// that field.
func KnownArgumentNamesRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Argument: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					var action = visitor.ActionNoChange
					var result interface{}
					if node, ok := p.Node.(*ast.Argument); ok {
						var argumentOf ast.Node
						if len(p.Ancestors) > 0 {
							argumentOf = p.Ancestors[len(p.Ancestors)-1]
						}
						if argumentOf == nil {
							return action, result
						}
						if argumentOf.GetKind() == kinds.Field {
							fieldDef := context.FieldDef()
							if fieldDef == nil {
								return action, result
							}
							nodeName := ""
							if node.Name != nil {
								nodeName = node.Name.Value
							}
							var fieldArgDef *Argument
							for _, arg := range fieldDef.Args {
								if arg.Name() == nodeName {
									fieldArgDef = arg
								}
							}
							if fieldArgDef == nil {
								parentType := context.ParentType()
								parentTypeName := ""
								if parentType != nil {
									parentTypeName = parentType.Name()
								}
								reportError(
									context,
									fmt.Sprintf(`Unknown argument "%v" on field "%v" of type "%v".`, nodeName, fieldDef.Name, parentTypeName),
									[]ast.Node{node},
								)
							}
						} else if argumentOf.GetKind() == kinds.Directive {
							directive := context.Directive()
							if directive == nil {
								return action, result
							}
							nodeName := ""
							if node.Name != nil {
								nodeName = node.Name.Value
							}
							var directiveArgDef *Argument
							for _, arg := range directive.Args {
								if arg.Name() == nodeName {
									directiveArgDef = arg
								}
							}
							if directiveArgDef == nil {
								reportError(
									context,
									fmt.Sprintf(`Unknown argument "%v" on directive "@%v".`, nodeName, directive.Name),
									[]ast.Node{node},
								)
							}
						}

					}
					return action, result
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func MisplaceDirectiveMessage(directiveName string, location string) string {
	return fmt.Sprintf(`Directive "%v" may not be used on %v.`, directiveName, location)
}

// KnownDirectivesRule Known directives
//
// A GraphQL document is only valid if all `@directives` are known by the
// schema and legally positioned.
func KnownDirectivesRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Directive: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					var action = visitor.ActionNoChange
					var result interface{}
					if node, ok := p.Node.(*ast.Directive); ok {

						nodeName := ""
						if node.Name != nil {
							nodeName = node.Name.Value
						}

						var directiveDef *Directive
						for _, def := range context.Schema().Directives() {
							if def.Name == nodeName {
								directiveDef = def
							}
						}
						if directiveDef == nil {
							return reportError(
								context,
								fmt.Sprintf(`Unknown directive "%v".`, nodeName),
								[]ast.Node{node},
							)
						}

						var appliedTo ast.Node
						if len(p.Ancestors) > 0 {
							appliedTo = p.Ancestors[len(p.Ancestors)-1]
						}
						if appliedTo == nil {
							return action, result
						}

						candidateLocation := getLocationForAppliedNode(appliedTo)

						directiveHasLocation := false
						for _, loc := range directiveDef.Locations {
							if loc == candidateLocation {
								directiveHasLocation = true
								break
							}
						}

						if candidateLocation == "" {
							reportError(
								context,
								MisplaceDirectiveMessage(nodeName, node.GetKind()),
								[]ast.Node{node},
							)
						} else if !directiveHasLocation {
							reportError(
								context,
								MisplaceDirectiveMessage(nodeName, candidateLocation),
								[]ast.Node{node},
							)
						}

					}
					return action, result
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func getLocationForAppliedNode(appliedTo ast.Node) string {
	kind := appliedTo.GetKind()
	if kind == kinds.OperationDefinition {
		appliedTo, _ := appliedTo.(*ast.OperationDefinition)
		if appliedTo.Operation == ast.OperationTypeQuery {
			return DirectiveLocationQuery
		}
		if appliedTo.Operation == ast.OperationTypeMutation {
			return DirectiveLocationMutation
		}
		if appliedTo.Operation == ast.OperationTypeSubscription {
			return DirectiveLocationSubscription
		}
	}
	if kind == kinds.Field {
		return DirectiveLocationField
	}
	if kind == kinds.FragmentSpread {
		return DirectiveLocationFragmentSpread
	}
	if kind == kinds.InlineFragment {
		return DirectiveLocationInlineFragment
	}
	if kind == kinds.FragmentDefinition {
		return DirectiveLocationFragmentDefinition
	}
	return ""
}

// KnownFragmentNamesRule Known fragment names
//
// A GraphQL document is only valid if all `...Fragment` fragment spreads refer
// to fragments defined in the same document.
func KnownFragmentNamesRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.FragmentSpread: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					var action = visitor.ActionNoChange
					var result interface{}
					if node, ok := p.Node.(*ast.FragmentSpread); ok {

						fragmentName := ""
						if node.Name != nil {
							fragmentName = node.Name.Value
						}

						fragment := context.Fragment(fragmentName)
						if fragment == nil {
							reportError(
								context,
								fmt.Sprintf(`Unknown fragment "%v".`, fragmentName),
								[]ast.Node{node.Name},
							)
						}
					}
					return action, result
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// KnownTypeNamesRule Known type names
//
// A GraphQL document is only valid if referenced types (specifically
// variable definitions and fragment conditions) are defined by the type schema.
func KnownTypeNamesRule(context *ValidationContext) *ValidationRuleInstance {
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.ObjectDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.InterfaceDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.UnionDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.InputObjectDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.Named: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Named); ok {
						typeNameValue := ""
						typeName := node.Name
						if typeName != nil {
							typeNameValue = typeName.Value
						}
						ttype := context.Schema().Type(typeNameValue)
						if ttype == nil {
							reportError(
								context,
								fmt.Sprintf(`Unknown type "%v".`, typeNameValue),
								[]ast.Node{node},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// LoneAnonymousOperationRule Lone anonymous operation
//
// A GraphQL document is only valid if when it contains an anonymous operation
// (the query short-hand) that it contains only that one operation definition.
func LoneAnonymousOperationRule(context *ValidationContext) *ValidationRuleInstance {
	var operationCount = 0
	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Document: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Document); ok {
						operationCount = 0
						for _, definition := range node.Definitions {
							if definition.GetKind() == kinds.OperationDefinition {
								operationCount = operationCount + 1
							}
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok {
						if node.Name == nil && operationCount > 1 {
							reportError(
								context,
								`This anonymous operation must be the only defined operation.`,
								[]ast.Node{node},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

type nodeSet struct {
	set map[ast.Node]bool
}

func newNodeSet() *nodeSet {
	return &nodeSet{
		set: map[ast.Node]bool{},
	}
}
func (set *nodeSet) Has(node ast.Node) bool {
	_, ok := set.set[node]
	return ok
}
func (set *nodeSet) Add(node ast.Node) bool {
	if set.Has(node) {
		return false
	}
	set.set[node] = true
	return true
}

func CycleErrorMessage(fragName string, spreadNames []string) string {
	via := ""
	if len(spreadNames) > 0 {
		via = " via " + strings.Join(spreadNames, ", ")
	}
	return fmt.Sprintf(`Cannot spread fragment "%v" within itself%v.`, fragName, via)
}

// NoFragmentCyclesRule No fragment cycles
func NoFragmentCyclesRule(context *ValidationContext) *ValidationRuleInstance {

	// Tracks already visited fragments to maintain O(N) and to ensure that cycles
	// are not redundantly reported.
	visitedFrags := map[string]bool{}

	// Array of AST nodes used to produce meaningful errors
	spreadPath := []*ast.FragmentSpread{}

	// Position in the spread path
	spreadPathIndexByName := map[string]int{}

	// This does a straight-forward DFS to find cycles.
	// It does not terminate when a cycle was found but continues to explore
	// the graph to find all possible cycles.
	var detectCycleRecursive func(fragment *ast.FragmentDefinition)
	detectCycleRecursive = func(fragment *ast.FragmentDefinition) {

		fragmentName := ""
		if fragment.Name != nil {
			fragmentName = fragment.Name.Value
		}
		visitedFrags[fragmentName] = true

		spreadNodes := context.FragmentSpreads(fragment)
		if len(spreadNodes) == 0 {
			return
		}

		spreadPathIndexByName[fragmentName] = len(spreadPath)

		for _, spreadNode := range spreadNodes {

			spreadName := ""
			if spreadNode.Name != nil {
				spreadName = spreadNode.Name.Value
			}
			cycleIndex, ok := spreadPathIndexByName[spreadName]
			if !ok {
				spreadPath = append(spreadPath, spreadNode)
				if visited, ok := visitedFrags[spreadName]; !ok || !visited {
					spreadFragment := context.Fragment(spreadName)
					if spreadFragment != nil {
						detectCycleRecursive(spreadFragment)
					}
				}
				spreadPath = spreadPath[:len(spreadPath)-1]
			} else {
				cyclePath := spreadPath[cycleIndex:]

				spreadNames := []string{}
				for _, s := range cyclePath {
					name := ""
					if s.Name != nil {
						name = s.Name.Value
					}
					spreadNames = append(spreadNames, name)
				}

				nodes := []ast.Node{}
				for _, c := range cyclePath {
					nodes = append(nodes, c)
				}
				nodes = append(nodes, spreadNode)

				reportError(
					context,
					CycleErrorMessage(spreadName, spreadNames),
					nodes,
				)
			}

		}
		delete(spreadPathIndexByName, fragmentName)

	}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.FragmentDefinition); ok && node != nil {
						nodeName := ""
						if node.Name != nil {
							nodeName = node.Name.Value
						}
						if _, ok := visitedFrags[nodeName]; !ok {
							detectCycleRecursive(node)
						}
					}
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func UndefinedVarMessage(varName string, opName string) string {
	if opName != "" {
		return fmt.Sprintf(`Variable "$%v" is not defined by operation "%v".`, varName, opName)
	}
	return fmt.Sprintf(`Variable "$%v" is not defined.`, varName)
}

// NoUndefinedVariablesRule No undefined variables
//
// A GraphQL operation is only valid if all variables encountered, both directly
// and via fragment spreads, are defined by that operation.
func NoUndefinedVariablesRule(context *ValidationContext) *ValidationRuleInstance {
	var variableNameDefined = map[string]bool{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					variableNameDefined = map[string]bool{}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if operation, ok := p.Node.(*ast.OperationDefinition); ok && operation != nil {
						usages := context.RecursiveVariableUsages(operation)

						for _, usage := range usages {
							if usage == nil {
								continue
							}
							if usage.Node == nil {
								continue
							}
							varName := ""
							if usage.Node.Name != nil {
								varName = usage.Node.Name.Value
							}
							opName := ""
							if operation.Name != nil {
								opName = operation.Name.Value
							}
							if res, ok := variableNameDefined[varName]; !ok || !res {
								reportError(
									context,
									UndefinedVarMessage(varName, opName),
									[]ast.Node{usage.Node, operation},
								)
							}
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.VariableDefinition); ok && node != nil {
						variableName := ""
						if node.Variable != nil && node.Variable.Name != nil {
							variableName = node.Variable.Name.Value
						}
						variableNameDefined[variableName] = true
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// NoUnusedFragmentsRule No unused fragments
//
// A GraphQL document is only valid if all fragment definitions are spread
// within operations, or spread within other fragments spread within operations.
func NoUnusedFragmentsRule(context *ValidationContext) *ValidationRuleInstance {

	var fragmentDefs = []*ast.FragmentDefinition{}
	var operationDefs = []*ast.OperationDefinition{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok && node != nil {
						operationDefs = append(operationDefs, node)
					}
					return visitor.ActionSkip, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.FragmentDefinition); ok && node != nil {
						fragmentDefs = append(fragmentDefs, node)
					}
					return visitor.ActionSkip, nil
				},
			},
			kinds.Document: {
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					fragmentNameUsed := map[string]bool{}
					for _, operation := range operationDefs {
						fragments := context.RecursivelyReferencedFragments(operation)
						for _, fragment := range fragments {
							fragName := ""
							if fragment.Name != nil {
								fragName = fragment.Name.Value
							}
							fragmentNameUsed[fragName] = true
						}
					}

					for _, def := range fragmentDefs {
						defName := ""
						if def.Name != nil {
							defName = def.Name.Value
						}

						isFragNameUsed, ok := fragmentNameUsed[defName]
						if !ok || isFragNameUsed != true {
							reportError(
								context,
								fmt.Sprintf(`Fragment "%v" is never used.`, defName),
								[]ast.Node{def},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func UnusedVariableMessage(varName string, opName string) string {
	if opName != "" {
		return fmt.Sprintf(`Variable "$%v" is never used in operation "%v".`, varName, opName)
	}
	return fmt.Sprintf(`Variable "$%v" is never used.`, varName)
}

// NoUnusedVariablesRule No unused variables
//
// A GraphQL operation is only valid if all variables defined by an operation
// are used, either directly or within a spread fragment.
func NoUnusedVariablesRule(context *ValidationContext) *ValidationRuleInstance {

	var variableDefs = []*ast.VariableDefinition{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					variableDefs = []*ast.VariableDefinition{}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if operation, ok := p.Node.(*ast.OperationDefinition); ok && operation != nil {
						variableNameUsed := map[string]bool{}
						usages := context.RecursiveVariableUsages(operation)

						for _, usage := range usages {
							varName := ""
							if usage != nil && usage.Node != nil && usage.Node.Name != nil {
								varName = usage.Node.Name.Value
							}
							if varName != "" {
								variableNameUsed[varName] = true
							}
						}
						for _, variableDef := range variableDefs {
							variableName := ""
							if variableDef != nil && variableDef.Variable != nil && variableDef.Variable.Name != nil {
								variableName = variableDef.Variable.Name.Value
							}
							opName := ""
							if operation.Name != nil {
								opName = operation.Name.Value
							}
							if res, ok := variableNameUsed[variableName]; !ok || !res {
								reportError(
									context,
									UnusedVariableMessage(variableName, opName),
									[]ast.Node{variableDef},
								)
							}
						}

					}

					return visitor.ActionNoChange, nil
				},
			},
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if def, ok := p.Node.(*ast.VariableDefinition); ok && def != nil {
						variableDefs = append(variableDefs, def)
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

type fieldDefPair struct {
	ParentType Composite
	Field      *ast.Field
	FieldDef   *FieldDefinition
}

func collectFieldASTsAndDefs(context *ValidationContext, parentType Named, selectionSet *ast.SelectionSet, visitedFragmentNames map[string]bool, astAndDefs map[string][]*fieldDefPair) map[string][]*fieldDefPair {

	if astAndDefs == nil {
		astAndDefs = map[string][]*fieldDefPair{}
	}
	if visitedFragmentNames == nil {
		visitedFragmentNames = map[string]bool{}
	}
	if selectionSet == nil {
		return astAndDefs
	}
	for _, selection := range selectionSet.Selections {
		switch selection := selection.(type) {
		case *ast.Field:
			fieldName := ""
			if selection.Name != nil {
				fieldName = selection.Name.Value
			}
			var fieldDef *FieldDefinition
			if parentType, ok := parentType.(*Object); ok {
				fieldDef, _ = parentType.Fields()[fieldName]
			}
			if parentType, ok := parentType.(*Interface); ok {
				fieldDef, _ = parentType.Fields()[fieldName]
			}

			responseName := fieldName
			if selection.Alias != nil {
				responseName = selection.Alias.Value
			}
			_, ok := astAndDefs[responseName]
			if !ok {
				astAndDefs[responseName] = []*fieldDefPair{}
			}
			if parentType, ok := parentType.(Composite); ok {
				astAndDefs[responseName] = append(astAndDefs[responseName], &fieldDefPair{
					ParentType: parentType,
					Field:      selection,
					FieldDef:   fieldDef,
				})
			} else {
				astAndDefs[responseName] = append(astAndDefs[responseName], &fieldDefPair{
					Field:    selection,
					FieldDef: fieldDef,
				})
			}
		case *ast.InlineFragment:
			inlineFragmentType := parentType
			if selection.TypeCondition != nil {
				parentType, _ := typeFromAST(*context.Schema(), selection.TypeCondition)
				inlineFragmentType = parentType
			}
			astAndDefs = collectFieldASTsAndDefs(
				context,
				inlineFragmentType,
				selection.SelectionSet,
				visitedFragmentNames,
				astAndDefs,
			)
		case *ast.FragmentSpread:
			fragName := ""
			if selection.Name != nil {
				fragName = selection.Name.Value
			}
			if _, ok := visitedFragmentNames[fragName]; ok {
				continue
			}
			visitedFragmentNames[fragName] = true
			fragment := context.Fragment(fragName)
			if fragment == nil {
				continue
			}
			parentType, _ := typeFromAST(*context.Schema(), fragment.TypeCondition)
			astAndDefs = collectFieldASTsAndDefs(
				context,
				parentType,
				fragment.SelectionSet,
				visitedFragmentNames,
				astAndDefs,
			)
		}
	}
	return astAndDefs
}

// pairSet A way to keep track of pairs of things when the ordering of the pair does
// not matter. We do this by maintaining a sort of double adjacency sets.
type pairSet struct {
	data map[ast.Node]*nodeSet
}

func newPairSet() *pairSet {
	return &pairSet{
		data: map[ast.Node]*nodeSet{},
	}
}
func (pair *pairSet) Has(a ast.Node, b ast.Node) bool {
	first, ok := pair.data[a]
	if !ok || first == nil {
		return false
	}
	res := first.Has(b)
	return res
}
func (pair *pairSet) Add(a ast.Node, b ast.Node) bool {
	pair.data = pairSetAdd(pair.data, a, b)
	pair.data = pairSetAdd(pair.data, b, a)
	return true
}

func pairSetAdd(data map[ast.Node]*nodeSet, a, b ast.Node) map[ast.Node]*nodeSet {
	set, ok := data[a]
	if !ok || set == nil {
		set = newNodeSet()
		data[a] = set
	}
	set.Add(b)
	return data
}

type conflictReason struct {
	Name    string
	Message interface{} // conflictReason || []conflictReason
}
type conflict struct {
	Reason      conflictReason
	FieldsLeft  []ast.Node
	FieldsRight []ast.Node
}

func sameArguments(args1 []*ast.Argument, args2 []*ast.Argument) bool {
	if len(args1) != len(args2) {
		return false
	}

	for _, arg1 := range args1 {
		arg1Name := ""
		if arg1.Name != nil {
			arg1Name = arg1.Name.Value
		}

		var foundArgs2 *ast.Argument
		for _, arg2 := range args2 {
			arg2Name := ""
			if arg2.Name != nil {
				arg2Name = arg2.Name.Value
			}
			if arg1Name == arg2Name {
				foundArgs2 = arg2
			}
			break
		}
		if foundArgs2 == nil {
			return false
		}
		if sameValue(arg1.Value, foundArgs2.Value) == false {
			return false
		}
	}

	return true
}
func sameValue(value1 ast.Value, value2 ast.Value) bool {
	if value1 == nil && value2 == nil {
		return true
	}
	val1 := printer.Print(value1)
	val2 := printer.Print(value2)

	return val1 == val2
}

func sameType(typeA, typeB Type) bool {
	if typeA == typeB {
		return true
	}

	if typeA, ok := typeA.(*List); ok {
		if typeB, ok := typeB.(*List); ok {
			return sameType(typeA.OfType, typeB.OfType)
		}
	}
	if typeA, ok := typeA.(*NonNull); ok {
		if typeB, ok := typeB.(*NonNull); ok {
			return sameType(typeA.OfType, typeB.OfType)
		}
	}

	return false
}

// Two types conflict if both types could not apply to a value simultaneously.
// Composite types are ignored as their individual field types will be compared
// later recursively. However List and Non-Null types must match.
func doTypesConflict(type1 Output, type2 Output) bool {
	if type1, ok := type1.(*List); ok {
		if type2, ok := type2.(*List); ok {
			return doTypesConflict(type1.OfType, type2.OfType)
		}
		return true
	}
	if type2, ok := type2.(*List); ok {
		if type1, ok := type1.(*List); ok {
			return doTypesConflict(type1.OfType, type2.OfType)
		}
		return true
	}
	if type1, ok := type1.(*NonNull); ok {
		if type2, ok := type2.(*NonNull); ok {
			return doTypesConflict(type1.OfType, type2.OfType)
		}
		return true
	}
	if type2, ok := type2.(*NonNull); ok {
		if type1, ok := type1.(*NonNull); ok {
			return doTypesConflict(type1.OfType, type2.OfType)
		}
		return true
	}
	if IsLeafType(type1) || IsLeafType(type2) {
		return type1 != type2
	}
	return false
}

// OverlappingFieldsCanBeMergedRule Overlapping fields can be merged
//
// A selection set is only valid if all fields (including spreading any
// fragments) either correspond to distinct response names or can be merged
// without ambiguity.
func OverlappingFieldsCanBeMergedRule(context *ValidationContext) *ValidationRuleInstance {

	var getSubfieldMap func(ast1 *ast.Field, type1 Output, ast2 *ast.Field, type2 Output) map[string][]*fieldDefPair
	var subfieldConflicts func(conflicts []*conflict, responseName string, ast1 *ast.Field, ast2 *ast.Field) *conflict
	var findConflicts func(parentFieldsAreMutuallyExclusive bool, fieldMap map[string][]*fieldDefPair) (conflicts []*conflict)

	comparedSet := newPairSet()
	findConflict := func(parentFieldsAreMutuallyExclusive bool, responseName string, field *fieldDefPair, field2 *fieldDefPair) *conflict {

		parentType1 := field.ParentType
		ast1 := field.Field
		def1 := field.FieldDef

		parentType2 := field2.ParentType
		ast2 := field2.Field
		def2 := field2.FieldDef

		// Not a pair.
		if ast1 == ast2 {
			return nil
		}

		// Memoize, do not report the same issue twice.
		// Note: Two overlapping ASTs could be encountered both when
		// `parentFieldsAreMutuallyExclusive` is true and is false, which could
		// produce different results (when `true` being a subset of `false`).
		// However we do not need to include this piece of information when
		// memoizing since this rule visits leaf fields before their parent fields,
		// ensuring that `parentFieldsAreMutuallyExclusive` is `false` the first
		// time two overlapping fields are encountered, ensuring that the full
		// set of validation rules are always checked when necessary.
		if comparedSet.Has(ast1, ast2) {
			return nil
		}
		comparedSet.Add(ast1, ast2)

		// The return type for each field.
		var type1 Type
		var type2 Type
		if def1 != nil {
			type1 = def1.Type
		}
		if def2 != nil {
			type2 = def2.Type
		}

		// If it is known that two fields could not possibly apply at the same
		// time, due to the parent types, then it is safe to permit them to diverge
		// in aliased field or arguments used as they will not present any ambiguity
		// by differing.
		// It is known that two parent types could never overlap if they are
		// different Object types. Interface or Union types might overlap - if not
		// in the current state of the schema, then perhaps in some future version,
		// thus may not safely diverge.
		_, isParentType1Object := parentType1.(*Object)
		_, isParentType2Object := parentType2.(*Object)
		fieldsAreMutuallyExclusive := parentFieldsAreMutuallyExclusive || parentType1 != parentType2 && isParentType1Object && isParentType2Object

		if !fieldsAreMutuallyExclusive {
			// Two aliases must refer to the same field.
			name1 := ""
			name2 := ""

			if ast1.Name != nil {
				name1 = ast1.Name.Value
			}
			if ast2.Name != nil {
				name2 = ast2.Name.Value
			}
			if name1 != name2 {
				return &conflict{
					Reason: conflictReason{
						Name:    responseName,
						Message: fmt.Sprintf(`%v and %v are different fields`, name1, name2),
					},
					FieldsLeft:  []ast.Node{ast1},
					FieldsRight: []ast.Node{ast2},
				}
			}

			// Two field calls must have the same arguments.
			if !sameArguments(ast1.Arguments, ast2.Arguments) {
				return &conflict{
					Reason: conflictReason{
						Name:    responseName,
						Message: `they have differing arguments`,
					},
					FieldsLeft:  []ast.Node{ast1},
					FieldsRight: []ast.Node{ast2},
				}
			}
		}

		if type1 != nil && type2 != nil && doTypesConflict(type1, type2) {
			return &conflict{
				Reason: conflictReason{
					Name:    responseName,
					Message: fmt.Sprintf(`they return conflicting types %v and %v`, type1, type2),
				},
				FieldsLeft:  []ast.Node{ast1},
				FieldsRight: []ast.Node{ast2},
			}
		}

		subFieldMap := getSubfieldMap(ast1, type1, ast2, type2)
		if subFieldMap != nil {
			conflicts := findConflicts(fieldsAreMutuallyExclusive, subFieldMap)
			return subfieldConflicts(conflicts, responseName, ast1, ast2)
		}

		return nil
	}

	getSubfieldMap = func(ast1 *ast.Field, type1 Output, ast2 *ast.Field, type2 Output) map[string][]*fieldDefPair {
		selectionSet1 := ast1.SelectionSet
		selectionSet2 := ast2.SelectionSet
		if selectionSet1 != nil && selectionSet2 != nil {
			visitedFragmentNames := map[string]bool{}
			subfieldMap := collectFieldASTsAndDefs(
				context,
				GetNamed(type1),
				selectionSet1,
				visitedFragmentNames,
				nil,
			)
			subfieldMap = collectFieldASTsAndDefs(
				context,
				GetNamed(type2),
				selectionSet2,
				visitedFragmentNames,
				subfieldMap,
			)
			return subfieldMap
		}
		return nil
	}

	subfieldConflicts = func(conflicts []*conflict, responseName string, ast1 *ast.Field, ast2 *ast.Field) *conflict {
		if len(conflicts) > 0 {
			conflictReasons := []conflictReason{}
			conflictFieldsLeft := []ast.Node{ast1}
			conflictFieldsRight := []ast.Node{ast2}
			for _, c := range conflicts {
				conflictReasons = append(conflictReasons, c.Reason)
				conflictFieldsLeft = append(conflictFieldsLeft, c.FieldsLeft...)
				conflictFieldsRight = append(conflictFieldsRight, c.FieldsRight...)
			}

			return &conflict{
				Reason: conflictReason{
					Name:    responseName,
					Message: conflictReasons,
				},
				FieldsLeft:  conflictFieldsLeft,
				FieldsRight: conflictFieldsRight,
			}
		}
		return nil
	}
	findConflicts = func(parentFieldsAreMutuallyExclusive bool, fieldMap map[string][]*fieldDefPair) (conflicts []*conflict) {

		// ensure field traversal
		orderedName := sort.StringSlice{}
		for responseName := range fieldMap {
			orderedName = append(orderedName, responseName)
		}
		orderedName.Sort()

		for _, responseName := range orderedName {
			fields, _ := fieldMap[responseName]
			for _, fieldA := range fields {
				for _, fieldB := range fields {
					c := findConflict(parentFieldsAreMutuallyExclusive, responseName, fieldA, fieldB)
					if c != nil {
						conflicts = append(conflicts, c)
					}
				}
			}
		}
		return conflicts
	}

	var reasonMessage func(message interface{}) string
	reasonMessage = func(message interface{}) string {
		switch reason := message.(type) {
		case string:
			return reason
		case conflictReason:
			return reasonMessage(reason.Message)
		case []conflictReason:
			messages := []string{}
			for _, r := range reason {
				messages = append(messages, fmt.Sprintf(
					`subfields "%v" conflict because %v`,
					r.Name,
					reasonMessage(r.Message),
				))
			}
			return strings.Join(messages, " and ")
		}
		return ""
	}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.SelectionSet: {
				// Note: we validate on the reverse traversal so deeper conflicts will be
				// caught first, for correct calculation of mutual exclusivity and for
				// clearer error messages.
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if selectionSet, ok := p.Node.(*ast.SelectionSet); ok && selectionSet != nil {
						parentType, _ := context.ParentType().(Named)
						fieldMap := collectFieldASTsAndDefs(
							context,
							parentType,
							selectionSet,
							nil,
							nil,
						)
						conflicts := findConflicts(false, fieldMap)
						if len(conflicts) > 0 {
							for _, c := range conflicts {
								responseName := c.Reason.Name
								reason := c.Reason
								reportError(
									context,
									fmt.Sprintf(
										`Fields "%v" conflict because %v.`,
										responseName,
										reasonMessage(reason),
									),
									append(c.FieldsLeft, c.FieldsRight...),
								)
							}
							return visitor.ActionNoChange, nil
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

func getFragmentType(context *ValidationContext, name string) Type {
	frag := context.Fragment(name)
	if frag == nil {
		return nil
	}
	ttype, _ := typeFromAST(*context.Schema(), frag.TypeCondition)
	return ttype
}

func doTypesOverlap(schema *Schema, t1 Type, t2 Type) bool {
	if t1 == t2 {
		return true
	}
	if _, ok := t1.(*Object); ok {
		if _, ok := t2.(*Object); ok {
			return false
		}
		if t2, ok := t2.(Abstract); ok {
			for _, ttype := range schema.PossibleTypes(t2) {
				if ttype == t1 {
					return true
				}
			}
			return false
		}
	}
	if t1, ok := t1.(Abstract); ok {
		if _, ok := t2.(*Object); ok {
			for _, ttype := range schema.PossibleTypes(t1) {
				if ttype == t2 {
					return true
				}
			}
			return false
		}
		t1TypeNames := map[string]bool{}
		for _, ttype := range schema.PossibleTypes(t1) {
			t1TypeNames[ttype.Name()] = true
		}
		if t2, ok := t2.(Abstract); ok {
			for _, ttype := range schema.PossibleTypes(t2) {
				if hasT1TypeName, _ := t1TypeNames[ttype.Name()]; hasT1TypeName {
					return true
				}
			}
			return false
		}
	}
	return false
}

// PossibleFragmentSpreadsRule Possible fragment spread
//
// A fragment spread is only valid if the type condition could ever possibly
// be true: if there is a non-empty intersection of the possible parent types,
// and possible types which pass the type condition.
func PossibleFragmentSpreadsRule(context *ValidationContext) *ValidationRuleInstance {

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.InlineFragment: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.InlineFragment); ok && node != nil {
						fragType := context.Type()
						parentType, _ := context.ParentType().(Type)

						if fragType != nil && parentType != nil && !doTypesOverlap(context.Schema(), fragType, parentType) {
							reportError(
								context,
								fmt.Sprintf(`Fragment cannot be spread here as objects of `+
									`type "%v" can never be of type "%v".`, parentType, fragType),
								[]ast.Node{node},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.FragmentSpread: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.FragmentSpread); ok && node != nil {
						fragName := ""
						if node.Name != nil {
							fragName = node.Name.Value
						}
						fragType := getFragmentType(context, fragName)
						parentType, _ := context.ParentType().(Type)
						if fragType != nil && parentType != nil && !doTypesOverlap(context.Schema(), fragType, parentType) {
							reportError(
								context,
								fmt.Sprintf(`Fragment "%v" cannot be spread here as objects of `+
									`type "%v" can never be of type "%v".`, fragName, parentType, fragType),
								[]ast.Node{node},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// ProvidedNonNullArgumentsRule Provided required arguments
//
// A field or directive is only valid if all required (non-null) field arguments
// have been provided.
func ProvidedNonNullArgumentsRule(context *ValidationContext) *ValidationRuleInstance {

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Field: {
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					// Validate on leave to allow for deeper errors to appear first.
					if fieldAST, ok := p.Node.(*ast.Field); ok && fieldAST != nil {
						fieldDef := context.FieldDef()
						if fieldDef == nil {
							return visitor.ActionSkip, nil
						}

						argASTs := fieldAST.Arguments

						argASTMap := map[string]*ast.Argument{}
						for _, arg := range argASTs {
							name := ""
							if arg.Name != nil {
								name = arg.Name.Value
							}
							argASTMap[name] = arg
						}
						for _, argDef := range fieldDef.Args {
							argAST, _ := argASTMap[argDef.Name()]
							if argAST == nil {
								if argDefType, ok := argDef.Type.(*NonNull); ok {
									fieldName := ""
									if fieldAST.Name != nil {
										fieldName = fieldAST.Name.Value
									}
									reportError(
										context,
										fmt.Sprintf(`Field "%v" argument "%v" of type "%v" `+
											`is required but not provided.`, fieldName, argDef.Name(), argDefType),
										[]ast.Node{fieldAST},
									)
								}
							}
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.Directive: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					// Validate on leave to allow for deeper errors to appear first.

					if directiveAST, ok := p.Node.(*ast.Directive); ok && directiveAST != nil {
						directiveDef := context.Directive()
						if directiveDef == nil {
							return visitor.ActionSkip, nil
						}
						argASTs := directiveAST.Arguments

						argASTMap := map[string]*ast.Argument{}
						for _, arg := range argASTs {
							name := ""
							if arg.Name != nil {
								name = arg.Name.Value
							}
							argASTMap[name] = arg
						}

						for _, argDef := range directiveDef.Args {
							argAST, _ := argASTMap[argDef.Name()]
							if argAST == nil {
								if argDefType, ok := argDef.Type.(*NonNull); ok {
									directiveName := ""
									if directiveAST.Name != nil {
										directiveName = directiveAST.Name.Value
									}
									reportError(
										context,
										fmt.Sprintf(`Directive "@%v" argument "%v" of type `+
											`"%v" is required but not provided.`, directiveName, argDef.Name(), argDefType),
										[]ast.Node{directiveAST},
									)
								}
							}
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// ScalarLeafsRule Scalar leafs
//
// A GraphQL document is valid only if all leaf fields (fields without
// sub selections) are of scalar or enum types.
func ScalarLeafsRule(context *ValidationContext) *ValidationRuleInstance {

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Field: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Field); ok && node != nil {
						nodeName := ""
						if node.Name != nil {
							nodeName = node.Name.Value
						}
						ttype := context.Type()
						if ttype != nil {
							if IsLeafType(ttype) {
								if node.SelectionSet != nil {
									reportError(
										context,
										fmt.Sprintf(`Field "%v" of type "%v" must not have a sub selection.`, nodeName, ttype),
										[]ast.Node{node.SelectionSet},
									)
								}
							} else if node.SelectionSet == nil {
								reportError(
									context,
									fmt.Sprintf(`Field "%v" of type "%v" must have a sub selection.`, nodeName, ttype),
									[]ast.Node{node},
								)
							}
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// UniqueArgumentNamesRule Unique argument names
//
// A GraphQL field or directive is only valid if all supplied arguments are
// uniquely named.
func UniqueArgumentNamesRule(context *ValidationContext) *ValidationRuleInstance {
	knownArgNames := map[string]*ast.Name{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.Field: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					knownArgNames = map[string]*ast.Name{}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.Directive: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					knownArgNames = map[string]*ast.Name{}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.Argument: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.Argument); ok {
						argName := ""
						if node.Name != nil {
							argName = node.Name.Value
						}
						if nameAST, ok := knownArgNames[argName]; ok {
							reportError(
								context,
								fmt.Sprintf(`There can be only one argument named "%v".`, argName),
								[]ast.Node{nameAST, node.Name},
							)
						} else {
							knownArgNames[argName] = node.Name
						}
					}
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// UniqueFragmentNamesRule Unique fragment names
//
// A GraphQL document is only valid if all defined fragments have unique names.
func UniqueFragmentNamesRule(context *ValidationContext) *ValidationRuleInstance {
	knownFragmentNames := map[string]*ast.Name{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.FragmentDefinition); ok && node != nil {
						fragmentName := ""
						if node.Name != nil {
							fragmentName = node.Name.Value
						}
						if nameAST, ok := knownFragmentNames[fragmentName]; ok {
							reportError(
								context,
								fmt.Sprintf(`There can only be one fragment named "%v".`, fragmentName),
								[]ast.Node{nameAST, node.Name},
							)
						} else {
							knownFragmentNames[fragmentName] = node.Name
						}
					}
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// UniqueInputFieldNamesRule Unique input field names
//
// A GraphQL input object value is only valid if all supplied fields are
// uniquely named.
func UniqueInputFieldNamesRule(context *ValidationContext) *ValidationRuleInstance {
	knownNameStack := []map[string]*ast.Name{}
	knownNames := map[string]*ast.Name{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.ObjectValue: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					knownNameStack = append(knownNameStack, knownNames)
					knownNames = map[string]*ast.Name{}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					// pop
					knownNames, knownNameStack = knownNameStack[len(knownNameStack)-1], knownNameStack[:len(knownNameStack)-1]
					return visitor.ActionNoChange, nil
				},
			},
			kinds.ObjectField: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.ObjectField); ok {
						fieldName := ""
						if node.Name != nil {
							fieldName = node.Name.Value
						}
						if knownNameAST, ok := knownNames[fieldName]; ok {
							reportError(
								context,
								fmt.Sprintf(`There can be only one input field named "%v".`, fieldName),
								[]ast.Node{knownNameAST, node.Name},
							)
						} else {
							knownNames[fieldName] = node.Name
						}

					}
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// UniqueOperationNamesRule Unique operation names
//
// A GraphQL document is only valid if all defined operations have unique names.
func UniqueOperationNamesRule(context *ValidationContext) *ValidationRuleInstance {
	knownOperationNames := map[string]*ast.Name{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok && node != nil {
						operationName := ""
						if node.Name != nil {
							operationName = node.Name.Value
						}
						if nameAST, ok := knownOperationNames[operationName]; ok {
							reportError(
								context,
								fmt.Sprintf(`There can only be one operation named "%v".`, operationName),
								[]ast.Node{nameAST, node.Name},
							)
						} else {
							knownOperationNames[operationName] = node.Name
						}
					}
					return visitor.ActionSkip, nil
				},
			},
			kinds.FragmentDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					return visitor.ActionSkip, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// UniqueVariableNamesRule Unique variable names
//
// A GraphQL operation is only valid if all its variables are uniquely named.
func UniqueVariableNamesRule(context *ValidationContext) *ValidationRuleInstance {
	knownVariableNames := map[string]*ast.Name{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.OperationDefinition); ok && node != nil {
						knownVariableNames = map[string]*ast.Name{}
					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.VariableDefinition); ok && node != nil {
						variableName := ""
						var variableNameAST *ast.Name
						if node.Variable != nil && node.Variable.Name != nil {
							variableNameAST = node.Variable.Name
							variableName = node.Variable.Name.Value
						}
						if nameAST, ok := knownVariableNames[variableName]; ok {
							reportError(
								context,
								fmt.Sprintf(`There can only be one variable named "%v".`, variableName),
								[]ast.Node{nameAST, variableNameAST},
							)
						} else {
							knownVariableNames[variableName] = variableNameAST
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// VariablesAreInputTypesRule Variables are input types
//
// A GraphQL operation is only valid if all the variables it defines are of
// input types (scalar, enum, or input object).
func VariablesAreInputTypesRule(context *ValidationContext) *ValidationRuleInstance {

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if node, ok := p.Node.(*ast.VariableDefinition); ok && node != nil {
						ttype, _ := typeFromAST(*context.Schema(), node.Type)

						// If the variable type is not an input type, return an error.
						if ttype != nil && !IsInputType(ttype) {
							variableName := ""
							if node.Variable != nil && node.Variable.Name != nil {
								variableName = node.Variable.Name.Value
							}
							reportError(
								context,
								fmt.Sprintf(`Variable "$%v" cannot be non-input type "%v".`,
									variableName, printer.Print(node.Type)),
								[]ast.Node{node.Type},
							)
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// If a variable definition has a default value, it's effectively non-null.
func effectiveType(varType Type, varDef *ast.VariableDefinition) Type {
	if varDef.DefaultValue == nil {
		return varType
	}
	if _, ok := varType.(*NonNull); ok {
		return varType
	}
	return NewNonNull(varType)
}

// VariablesInAllowedPositionRule Variables passed to field arguments conform to type
func VariablesInAllowedPositionRule(context *ValidationContext) *ValidationRuleInstance {

	varDefMap := map[string]*ast.VariableDefinition{}

	visitorOpts := &visitor.VisitorOptions{
		KindFuncMap: map[string]visitor.NamedVisitFuncs{
			kinds.OperationDefinition: {
				Enter: func(p visitor.VisitFuncParams) (string, interface{}) {
					varDefMap = map[string]*ast.VariableDefinition{}
					return visitor.ActionNoChange, nil
				},
				Leave: func(p visitor.VisitFuncParams) (string, interface{}) {
					if operation, ok := p.Node.(*ast.OperationDefinition); ok {

						usages := context.RecursiveVariableUsages(operation)
						for _, usage := range usages {
							varName := ""
							if usage != nil && usage.Node != nil && usage.Node.Name != nil {
								varName = usage.Node.Name.Value
							}
							varDef, _ := varDefMap[varName]
							if varDef != nil && usage.Type != nil {
								varType, err := typeFromAST(*context.Schema(), varDef.Type)
								if err != nil {
									varType = nil
								}
								if varType != nil && !isTypeSubTypeOf(context.Schema(), effectiveType(varType, varDef), usage.Type) {
									reportError(
										context,
										fmt.Sprintf(`Variable "$%v" of type "%v" used in position `+
											`expecting type "%v".`, varName, varType, usage.Type),
										[]ast.Node{varDef, usage.Node},
									)
								}
							}
						}

					}
					return visitor.ActionNoChange, nil
				},
			},
			kinds.VariableDefinition: {
				Kind: func(p visitor.VisitFuncParams) (string, interface{}) {
					if varDefAST, ok := p.Node.(*ast.VariableDefinition); ok {
						defName := ""
						if varDefAST.Variable != nil && varDefAST.Variable.Name != nil {
							defName = varDefAST.Variable.Name.Value
						}
						if defName != "" {
							varDefMap[defName] = varDefAST
						}
					}
					return visitor.ActionNoChange, nil
				},
			},
		},
	}
	return &ValidationRuleInstance{
		VisitorOpts: visitorOpts,
	}
}

// Utility for validators which determines if a value literal AST is valid given
// an input type.
//
// Note that this only validates literal values, variables are assumed to
// provide values of the correct type.
func isValidLiteralValue(ttype Input, valueAST ast.Value) (bool, []string) {
	// A value must be provided if the type is non-null.
	if ttype, ok := ttype.(*NonNull); ok {
		if valueAST == nil {
			if ttype.OfType.Name() != "" {
				return false, []string{fmt.Sprintf(`Expected "%v!", found null.`, ttype.OfType.Name())}
			}
			return false, []string{"Expected non-null value, found null."}
		}
		ofType, _ := ttype.OfType.(Input)
		return isValidLiteralValue(ofType, valueAST)
	}

	if valueAST == nil {
		return true, nil
	}

	// This function only tests literals, and assumes variables will provide
	// values of the correct type.
	if valueAST.GetKind() == kinds.Variable {
		return true, nil
	}

	// Lists accept a non-list value as a list of one.
	if ttype, ok := ttype.(*List); ok {
		itemType, _ := ttype.OfType.(Input)
		if valueAST, ok := valueAST.(*ast.ListValue); ok {
			messagesReduce := []string{}
			for _, value := range valueAST.Values {
				_, messages := isValidLiteralValue(itemType, value)
				for idx, message := range messages {
					messagesReduce = append(messagesReduce, fmt.Sprintf(`In element #%v: %v`, idx+1, message))
				}
			}
			return (len(messagesReduce) == 0), messagesReduce
		}
		return isValidLiteralValue(itemType, valueAST)

	}

	// Input objects check each defined field and look for undefined fields.
	if ttype, ok := ttype.(*InputObject); ok {
		valueAST, ok := valueAST.(*ast.ObjectValue)
		if !ok {
			return false, []string{fmt.Sprintf(`Expected "%v", found not an object.`, ttype.Name())}
		}
		fields := ttype.Fields()
		messagesReduce := []string{}

		// Ensure every provided field is defined.
		fieldASTs := valueAST.Fields
		fieldASTMap := map[string]*ast.ObjectField{}
		for _, fieldAST := range fieldASTs {
			fieldASTName := ""
			if fieldAST.Name != nil {
				fieldASTName = fieldAST.Name.Value
			}

			fieldASTMap[fieldASTName] = fieldAST

			field, ok := fields[fieldASTName]
			if !ok || field == nil {
				messagesReduce = append(messagesReduce, fmt.Sprintf(`In field "%v": Unknown field.`, fieldASTName))
			}
		}
		// Ensure every defined field is valid.
		for fieldName, field := range fields {
			fieldAST, _ := fieldASTMap[fieldName]
			var fieldASTValue ast.Value
			if fieldAST != nil {
				fieldASTValue = fieldAST.Value
			}
			if isValid, messages := isValidLiteralValue(field.Type, fieldASTValue); !isValid {
				for _, message := range messages {
					messagesReduce = append(messagesReduce, fmt.Sprintf("In field \"%v\": %v", fieldName, message))
				}
			}
		}
		return (len(messagesReduce) == 0), messagesReduce
	}

	if ttype, ok := ttype.(*Scalar); ok {
		if isNullish(ttype.ParseLiteral(valueAST)) {
			return false, []string{fmt.Sprintf(`Expected type "%v", found %v.`, ttype.Name(), printer.Print(valueAST))}
		}
	}
	if ttype, ok := ttype.(*Enum); ok {
		if isNullish(ttype.ParseLiteral(valueAST)) {
			return false, []string{fmt.Sprintf(`Expected type "%v", found %v.`, ttype.Name(), printer.Print(valueAST))}
		}
	}

	return true, nil
}
