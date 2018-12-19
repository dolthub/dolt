package graphql

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/attic-labs/graphql/language/ast"
	"golang.org/x/net/context"
)

// Type interface for all of the possible kinds of GraphQL types
type Type interface {
	Name() string
	Description() string
	String() string
	Error() error
}

var _ Type = (*Scalar)(nil)
var _ Type = (*Object)(nil)
var _ Type = (*Interface)(nil)
var _ Type = (*Union)(nil)
var _ Type = (*Enum)(nil)
var _ Type = (*InputObject)(nil)
var _ Type = (*List)(nil)
var _ Type = (*NonNull)(nil)
var _ Type = (*Argument)(nil)

// Input interface for types that may be used as input types for arguments and directives.
type Input interface {
	Name() string
	Description() string
	String() string
	Error() error
}

var _ Input = (*Scalar)(nil)
var _ Input = (*Enum)(nil)
var _ Input = (*InputObject)(nil)
var _ Input = (*List)(nil)
var _ Input = (*NonNull)(nil)

// IsInputType determines if given type is a GraphQLInputType
func IsInputType(ttype Type) bool {
	named := GetNamed(ttype)
	if _, ok := named.(*Scalar); ok {
		return true
	}
	if _, ok := named.(*Enum); ok {
		return true
	}
	if _, ok := named.(*InputObject); ok {
		return true
	}
	return false
}

// IsOutputType determines if given type is a GraphQLOutputType
func IsOutputType(ttype Type) bool {
	name := GetNamed(ttype)
	if _, ok := name.(*Scalar); ok {
		return true
	}
	if _, ok := name.(*Object); ok {
		return true
	}
	if _, ok := name.(*Interface); ok {
		return true
	}
	if _, ok := name.(*Union); ok {
		return true
	}
	if _, ok := name.(*Enum); ok {
		return true
	}
	return false
}

// Leaf interface for types that may be leaf values
type Leaf interface {
	Name() string
	Description() string
	String() string
	Error() error
	Serialize(value interface{}) interface{}
}

var _ Leaf = (*Scalar)(nil)
var _ Leaf = (*Enum)(nil)

// IsLeafType determines if given type is a leaf value
func IsLeafType(ttype Type) bool {
	named := GetNamed(ttype)
	if _, ok := named.(*Scalar); ok {
		return true
	}
	if _, ok := named.(*Enum); ok {
		return true
	}
	return false
}

// Output interface for types that may be used as output types as the result of fields.
type Output interface {
	Name() string
	Description() string
	String() string
	Error() error
}

var _ Output = (*Scalar)(nil)
var _ Output = (*Object)(nil)
var _ Output = (*Interface)(nil)
var _ Output = (*Union)(nil)
var _ Output = (*Enum)(nil)
var _ Output = (*List)(nil)
var _ Output = (*NonNull)(nil)

// Composite interface for types that may describe the parent context of a selection set.
type Composite interface {
	Name() string
}

var _ Composite = (*Object)(nil)
var _ Composite = (*Interface)(nil)
var _ Composite = (*Union)(nil)

// IsCompositeType determines if given type is a GraphQLComposite type
func IsCompositeType(ttype interface{}) bool {
	if _, ok := ttype.(*Object); ok {
		return true
	}
	if _, ok := ttype.(*Interface); ok {
		return true
	}
	if _, ok := ttype.(*Union); ok {
		return true
	}
	return false
}

// Abstract interface for types that may describe the parent context of a selection set.
type Abstract interface {
	Name() string
}

var _ Abstract = (*Interface)(nil)
var _ Abstract = (*Union)(nil)

func IsAbstractType(ttype interface{}) bool {
	if _, ok := ttype.(*Interface); ok {
		return true
	}
	if _, ok := ttype.(*Union); ok {
		return true
	}
	return false
}

// Nullable interface for types that can accept null as a value.
type Nullable interface {
}

var _ Nullable = (*Scalar)(nil)
var _ Nullable = (*Object)(nil)
var _ Nullable = (*Interface)(nil)
var _ Nullable = (*Union)(nil)
var _ Nullable = (*Enum)(nil)
var _ Nullable = (*InputObject)(nil)
var _ Nullable = (*List)(nil)

// GetNullable returns the Nullable type of the given GraphQL type
func GetNullable(ttype Type) Nullable {
	if ttype, ok := ttype.(*NonNull); ok {
		return ttype.OfType
	}
	return ttype
}

// Named interface for types that do not include modifiers like List or NonNull.
type Named interface {
	String() string
}

var _ Named = (*Scalar)(nil)
var _ Named = (*Object)(nil)
var _ Named = (*Interface)(nil)
var _ Named = (*Union)(nil)
var _ Named = (*Enum)(nil)
var _ Named = (*InputObject)(nil)

// GetNamed returns the Named type of the given GraphQL type
func GetNamed(ttype Type) Named {
	unmodifiedType := ttype
	for {
		if ttype, ok := unmodifiedType.(*List); ok {
			unmodifiedType = ttype.OfType
			continue
		}
		if ttype, ok := unmodifiedType.(*NonNull); ok {
			unmodifiedType = ttype.OfType
			continue
		}
		break
	}
	return unmodifiedType
}

// Scalar Type Definition
//
// The leaf values of any request and input values to arguments are
// Scalars (or Enums) and are defined with a name and a series of functions
// used to parse input from ast or variables and to ensure validity.
//
// Example:
//
//    var OddType = new Scalar({
//      name: 'Odd',
//      serialize(value) {
//        return value % 2 === 1 ? value : null;
//      }
//    });
//
type Scalar struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`

	scalarConfig ScalarConfig
	err          error
}

// SerializeFn is a function type for serializing a GraphQLScalar type value
type SerializeFn func(value interface{}) interface{}

// ParseValueFn is a function type for parsing the value of a GraphQLScalar type
type ParseValueFn func(value interface{}) interface{}

// ParseLiteralFn is a function type for parsing the literal value of a GraphQLScalar type
type ParseLiteralFn func(valueAST ast.Value) interface{}

// ScalarConfig options for creating a new GraphQLScalar
type ScalarConfig struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Serialize    SerializeFn
	ParseValue   ParseValueFn
	ParseLiteral ParseLiteralFn
}

// NewScalar creates a new GraphQLScalar
func NewScalar(config ScalarConfig) *Scalar {
	st := &Scalar{}
	err := invariant(config.Name != "", "Type must be named.")
	if err != nil {
		st.err = err
		return st
	}

	err = assertValidName(config.Name)
	if err != nil {
		st.err = err
		return st
	}

	st.PrivateName = config.Name
	st.PrivateDescription = config.Description

	err = invariant(
		config.Serialize != nil,
		fmt.Sprintf(`%v must provide "serialize" function. If this custom Scalar is `+
			`also used as an input type, ensure "parseValue" and "parseLiteral" `+
			`functions are also provided.`, st),
	)
	if err != nil {
		st.err = err
		return st
	}
	if config.ParseValue != nil || config.ParseLiteral != nil {
		err = invariant(
			config.ParseValue != nil && config.ParseLiteral != nil,
			fmt.Sprintf(`%v must provide both "parseValue" and "parseLiteral" functions.`, st),
		)
		if err != nil {
			st.err = err
			return st
		}
	}

	st.scalarConfig = config
	return st
}
func (st *Scalar) Serialize(value interface{}) interface{} {
	if st.scalarConfig.Serialize == nil {
		return value
	}
	return st.scalarConfig.Serialize(value)
}
func (st *Scalar) ParseValue(value interface{}) interface{} {
	if st.scalarConfig.ParseValue == nil {
		return value
	}
	return st.scalarConfig.ParseValue(value)
}
func (st *Scalar) ParseLiteral(valueAST ast.Value) interface{} {
	if st.scalarConfig.ParseLiteral == nil {
		return nil
	}
	return st.scalarConfig.ParseLiteral(valueAST)
}
func (st *Scalar) Name() string {
	return st.PrivateName
}
func (st *Scalar) Description() string {
	return st.PrivateDescription

}
func (st *Scalar) String() string {
	return st.PrivateName
}
func (st *Scalar) Error() error {
	return st.err
}

// Object Type Definition
//
// Almost all of the GraphQL types you define will be object  Object types
// have a name, but most importantly describe their fields.
// Example:
//
//    var AddressType = new Object({
//      name: 'Address',
//      fields: {
//        street: { type: String },
//        number: { type: Int },
//        formatted: {
//          type: String,
//          resolve(obj) {
//            return obj.number + ' ' + obj.street
//          }
//        }
//      }
//    });
//
// When two types need to refer to each other, or a type needs to refer to
// itself in a field, you can use a function expression (aka a closure or a
// thunk) to supply the fields lazily.
//
// Example:
//
//    var PersonType = new Object({
//      name: 'Person',
//      fields: () => ({
//        name: { type: String },
//        bestFriend: { type: PersonType },
//      })
//    });
//
// /
type Object struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`
	IsTypeOf           IsTypeOfFn

	typeConfig ObjectConfig
	fields     FieldDefinitionMap
	interfaces []*Interface
	// Interim alternative to throwing an error during schema definition at run-time
	err error
}

// IsTypeOfParams Params for IsTypeOfFn()
type IsTypeOfParams struct {
	// Value that needs to be resolve.
	// Use this to decide which GraphQLObject this value maps to.
	Value interface{}

	// Info is a collection of information about the current execution state.
	Info ResolveInfo

	// Context argument is a context value that is provided to every resolve function within an execution.
	// It is commonly
	// used to represent an authenticated user, or request-specific caches.
	Context context.Context
}

type IsTypeOfFn func(p IsTypeOfParams) bool

type InterfacesThunk func() []*Interface

type ObjectConfig struct {
	Name        string      `json:"name"`
	Interfaces  interface{} `json:"interfaces"`
	Fields      interface{} `json:"fields"`
	IsTypeOf    IsTypeOfFn  `json:"isTypeOf"`
	Description string      `json:"description"`
}
type FieldsThunk func() Fields

func NewObject(config ObjectConfig) *Object {
	objectType := &Object{}

	err := invariant(config.Name != "", "Type must be named.")
	if err != nil {
		objectType.err = err
		return objectType
	}
	err = assertValidName(config.Name)
	if err != nil {
		objectType.err = err
		return objectType
	}

	objectType.PrivateName = config.Name
	objectType.PrivateDescription = config.Description
	objectType.IsTypeOf = config.IsTypeOf
	objectType.typeConfig = config

	return objectType
}
func (gt *Object) AddFieldConfig(fieldName string, fieldConfig *Field) {
	if fieldName == "" || fieldConfig == nil {
		return
	}
	switch gt.typeConfig.Fields.(type) {
	case Fields:
		gt.typeConfig.Fields.(Fields)[fieldName] = fieldConfig
	}
}
func (gt *Object) Name() string {
	return gt.PrivateName
}
func (gt *Object) Description() string {
	return ""
}
func (gt *Object) String() string {
	return gt.PrivateName
}
func (gt *Object) Fields() FieldDefinitionMap {
	var configureFields Fields
	switch gt.typeConfig.Fields.(type) {
	case Fields:
		configureFields = gt.typeConfig.Fields.(Fields)
	case FieldsThunk:
		configureFields = gt.typeConfig.Fields.(FieldsThunk)()
	}
	fields, err := defineFieldMap(gt, configureFields)
	gt.err = err
	gt.fields = fields
	return gt.fields
}

func (gt *Object) Interfaces() []*Interface {
	var configInterfaces []*Interface
	switch gt.typeConfig.Interfaces.(type) {
	case InterfacesThunk:
		configInterfaces = gt.typeConfig.Interfaces.(InterfacesThunk)()
	case []*Interface:
		configInterfaces = gt.typeConfig.Interfaces.([]*Interface)
	case nil:
	default:
		gt.err = fmt.Errorf("Unknown Object.Interfaces type: %v", reflect.TypeOf(gt.typeConfig.Interfaces))
		return nil
	}
	interfaces, err := defineInterfaces(gt, configInterfaces)
	gt.err = err
	gt.interfaces = interfaces
	return gt.interfaces
}
func (gt *Object) Error() error {
	return gt.err
}

func defineInterfaces(ttype *Object, interfaces []*Interface) ([]*Interface, error) {
	ifaces := []*Interface{}

	if len(interfaces) == 0 {
		return ifaces, nil
	}
	for _, iface := range interfaces {
		err := invariant(
			iface != nil,
			fmt.Sprintf(`%v may only implement Interface types, it cannot implement: %v.`, ttype, iface),
		)
		if err != nil {
			return ifaces, err
		}
		if iface.ResolveType != nil {
			err = invariant(
				iface.ResolveType != nil,
				fmt.Sprintf(`Interface Type %v does not provide a "resolveType" function `+
					`and implementing Type %v does not provide a "isTypeOf" `+
					`function. There is no way to resolve this implementing type `+
					`during execution.`, iface, ttype),
			)
			if err != nil {
				return ifaces, err
			}
		}
		ifaces = append(ifaces, iface)
	}

	return ifaces, nil
}

func defineFieldMap(ttype Named, fields Fields) (FieldDefinitionMap, error) {

	resultFieldMap := FieldDefinitionMap{}

	err := invariant(
		len(fields) > 0,
		fmt.Sprintf(`%v fields must be an object with field names as keys or a function which return such an object.`, ttype),
	)
	if err != nil {
		return resultFieldMap, err
	}

	for fieldName, field := range fields {
		if field == nil {
			continue
		}
		err = invariant(
			field.Type != nil,
			fmt.Sprintf(`%v.%v field type must be Output Type but got: %v.`, ttype, fieldName, field.Type),
		)
		if err != nil {
			return resultFieldMap, err
		}
		if field.Type.Error() != nil {
			return resultFieldMap, field.Type.Error()
		}
		err = assertValidName(fieldName)
		if err != nil {
			return resultFieldMap, err
		}
		fieldDef := &FieldDefinition{
			Name:              fieldName,
			Description:       field.Description,
			Type:              field.Type,
			Resolve:           field.Resolve,
			DeprecationReason: field.DeprecationReason,
		}

		fieldDef.Args = []*Argument{}
		for argName, arg := range field.Args {
			err := assertValidName(argName)
			if err != nil {
				return resultFieldMap, err
			}
			err = invariant(
				arg != nil,
				fmt.Sprintf(`%v.%v args must be an object with argument names as keys.`, ttype, fieldName),
			)
			if err != nil {
				return resultFieldMap, err
			}
			err = invariant(
				arg.Type != nil,
				fmt.Sprintf(`%v.%v(%v:) argument type must be Input Type but got: %v.`, ttype, fieldName, argName, arg.Type),
			)
			if err != nil {
				return resultFieldMap, err
			}
			fieldArg := &Argument{
				PrivateName:        argName,
				PrivateDescription: arg.Description,
				Type:               arg.Type,
				DefaultValue:       arg.DefaultValue,
			}
			fieldDef.Args = append(fieldDef.Args, fieldArg)
		}
		resultFieldMap[fieldName] = fieldDef
	}
	return resultFieldMap, nil
}

// ResolveParams Params for FieldResolveFn()
type ResolveParams struct {
	// Source is the source value
	Source interface{}

	// Args is a map of arguments for current GraphQL request
	Args map[string]interface{}

	// Info is a collection of information about the current execution state.
	Info ResolveInfo

	// Context argument is a context value that is provided to every resolve function within an execution.
	// It is commonly
	// used to represent an authenticated user, or request-specific caches.
	Context context.Context
}

type FieldResolveFn func(p ResolveParams) (interface{}, error)

type ResolveInfo struct {
	FieldName      string
	FieldASTs      []*ast.Field
	ReturnType     Output
	ParentType     Composite
	Schema         Schema
	Fragments      map[string]ast.Definition
	RootValue      interface{}
	Operation      ast.Definition
	VariableValues map[string]interface{}
}

type Fields map[string]*Field

type Field struct {
	Name              string              `json:"name"` // used by graphlql-relay
	Type              Output              `json:"type"`
	Args              FieldConfigArgument `json:"args"`
	Resolve           FieldResolveFn
	DeprecationReason string `json:"deprecationReason"`
	Description       string `json:"description"`
}

type FieldConfigArgument map[string]*ArgumentConfig

type ArgumentConfig struct {
	Type         Input       `json:"type"`
	DefaultValue interface{} `json:"defaultValue"`
	Description  string      `json:"description"`
}

type FieldDefinitionMap map[string]*FieldDefinition
type FieldDefinition struct {
	Name              string         `json:"name"`
	Description       string         `json:"description"`
	Type              Output         `json:"type"`
	Args              []*Argument    `json:"args"`
	Resolve           FieldResolveFn `json:"-"`
	DeprecationReason string         `json:"deprecationReason"`
}

type FieldArgument struct {
	Name         string      `json:"name"`
	Type         Type        `json:"type"`
	DefaultValue interface{} `json:"defaultValue"`
	Description  string      `json:"description"`
}

type Argument struct {
	PrivateName        string      `json:"name"`
	Type               Input       `json:"type"`
	DefaultValue       interface{} `json:"defaultValue"`
	PrivateDescription string      `json:"description"`
}

func (st *Argument) Name() string {
	return st.PrivateName
}
func (st *Argument) Description() string {
	return st.PrivateDescription

}
func (st *Argument) String() string {
	return st.PrivateName
}
func (st *Argument) Error() error {
	return nil
}

// Interface Type Definition
//
// When a field can return one of a heterogeneous set of types, a Interface type
// is used to describe what types are possible, what fields are in common across
// all types, as well as a function to determine which type is actually used
// when the field is resolved.
//
// Example:
//
//     var EntityType = new Interface({
//       name: 'Entity',
//       fields: {
//         name: { type: String }
//       }
//     });
//
//
type Interface struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`
	ResolveType        ResolveTypeFn

	typeConfig InterfaceConfig
	fields     FieldDefinitionMap
	err        error
}
type InterfaceConfig struct {
	Name        string      `json:"name"`
	Fields      interface{} `json:"fields"`
	ResolveType ResolveTypeFn
	Description string `json:"description"`
}

// ResolveTypeParams Params for ResolveTypeFn()
type ResolveTypeParams struct {
	// Value that needs to be resolve.
	// Use this to decide which GraphQLObject this value maps to.
	Value interface{}

	// Info is a collection of information about the current execution state.
	Info ResolveInfo

	// Context argument is a context value that is provided to every resolve function within an execution.
	// It is commonly
	// used to represent an authenticated user, or request-specific caches.
	Context context.Context
}

type ResolveTypeFn func(p ResolveTypeParams) *Object

func NewInterface(config InterfaceConfig) *Interface {
	it := &Interface{}

	err := invariant(config.Name != "", "Type must be named.")
	if err != nil {
		it.err = err
		return it
	}
	err = assertValidName(config.Name)
	if err != nil {
		it.err = err
		return it
	}
	it.PrivateName = config.Name
	it.PrivateDescription = config.Description
	it.ResolveType = config.ResolveType
	it.typeConfig = config

	return it
}

func (it *Interface) AddFieldConfig(fieldName string, fieldConfig *Field) {
	if fieldName == "" || fieldConfig == nil {
		return
	}
	switch it.typeConfig.Fields.(type) {
	case Fields:
		it.typeConfig.Fields.(Fields)[fieldName] = fieldConfig
	}
}
func (it *Interface) Name() string {
	return it.PrivateName
}
func (it *Interface) Description() string {
	return it.PrivateDescription
}
func (it *Interface) Fields() (fields FieldDefinitionMap) {
	var configureFields Fields
	switch it.typeConfig.Fields.(type) {
	case Fields:
		configureFields = it.typeConfig.Fields.(Fields)
	case FieldsThunk:
		configureFields = it.typeConfig.Fields.(FieldsThunk)()
	}
	fields, err := defineFieldMap(it, configureFields)
	it.err = err
	it.fields = fields
	return it.fields
}
func (it *Interface) String() string {
	return it.PrivateName
}
func (it *Interface) Error() error {
	return it.err
}

// Union Type Definition
//
// When a field can return one of a heterogeneous set of types, a Union type
// is used to describe what types are possible as well as providing a function
// to determine which type is actually used when the field is resolved.
//
// Example:
//
//     var PetType = new Union({
//       name: 'Pet',
//       types: [ DogType, CatType ],
//       resolveType(value) {
//         if (value instanceof Dog) {
//           return DogType;
//         }
//         if (value instanceof Cat) {
//           return CatType;
//         }
//       }
//     });
type Union struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`
	ResolveType        ResolveTypeFn

	typeConfig    UnionConfig
	types         []*Object
	possibleTypes map[string]bool

	err error
}
type UnionConfig struct {
	Name        string    `json:"name"`
	Types       []*Object `json:"types"`
	ResolveType ResolveTypeFn
	Description string `json:"description"`
}

func NewUnion(config UnionConfig) *Union {
	objectType := &Union{}

	err := invariant(config.Name != "", "Type must be named.")
	if err != nil {
		objectType.err = err
		return objectType
	}
	err = assertValidName(config.Name)
	if err != nil {
		objectType.err = err
		return objectType
	}
	objectType.PrivateName = config.Name
	objectType.PrivateDescription = config.Description
	objectType.ResolveType = config.ResolveType

	err = invariant(
		len(config.Types) > 0,
		fmt.Sprintf(`Must provide Array of types for Union %v.`, config.Name),
	)
	if err != nil {
		objectType.err = err
		return objectType
	}
	for _, ttype := range config.Types {
		err := invariant(
			ttype != nil,
			fmt.Sprintf(`%v may only contain Object types, it cannot contain: %v.`, objectType, ttype),
		)
		if err != nil {
			objectType.err = err
			return objectType
		}
		if objectType.ResolveType == nil {
			err = invariant(
				ttype.IsTypeOf != nil,
				fmt.Sprintf(`Union Type %v does not provide a "resolveType" function `+
					`and possible Type %v does not provide a "isTypeOf" `+
					`function. There is no way to resolve this possible type `+
					`during execution.`, objectType, ttype),
			)
			if err != nil {
				objectType.err = err
				return objectType
			}
		}
	}
	objectType.types = config.Types
	objectType.typeConfig = config

	return objectType
}
func (ut *Union) Types() []*Object {
	return ut.types
}
func (ut *Union) String() string {
	return ut.PrivateName
}
func (ut *Union) Name() string {
	return ut.PrivateName
}
func (ut *Union) Description() string {
	return ut.PrivateDescription
}
func (ut *Union) Error() error {
	return ut.err
}

// Enum Type Definition
//
// Some leaf values of requests and input values are Enums. GraphQL serializes
// Enum values as strings, however internally Enums can be represented by any
// kind of type, often integers.
//
// Example:
//
//     var RGBType = new Enum({
//       name: 'RGB',
//       values: {
//         RED: { value: 0 },
//         GREEN: { value: 1 },
//         BLUE: { value: 2 }
//       }
//     });
//
// Note: If a value is not provided in a definition, the name of the enum value
// will be used as its internal value.

type Enum struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`

	enumConfig   EnumConfig
	values       []*EnumValueDefinition
	valuesLookup map[interface{}]*EnumValueDefinition
	nameLookup   map[string]*EnumValueDefinition

	err error
}
type EnumValueConfigMap map[string]*EnumValueConfig
type EnumValueConfig struct {
	Value             interface{} `json:"value"`
	DeprecationReason string      `json:"deprecationReason"`
	Description       string      `json:"description"`
}
type EnumConfig struct {
	Name        string             `json:"name"`
	Values      EnumValueConfigMap `json:"values"`
	Description string             `json:"description"`
}
type EnumValueDefinition struct {
	Name              string      `json:"name"`
	Value             interface{} `json:"value"`
	DeprecationReason string      `json:"deprecationReason"`
	Description       string      `json:"description"`
}

func NewEnum(config EnumConfig) *Enum {
	gt := &Enum{}
	gt.enumConfig = config

	err := assertValidName(config.Name)
	if err != nil {
		gt.err = err
		return gt
	}

	gt.PrivateName = config.Name
	gt.PrivateDescription = config.Description
	gt.values, err = gt.defineEnumValues(config.Values)
	if err != nil {
		gt.err = err
		return gt
	}

	return gt
}
func (gt *Enum) defineEnumValues(valueMap EnumValueConfigMap) ([]*EnumValueDefinition, error) {
	values := []*EnumValueDefinition{}

	err := invariant(
		len(valueMap) > 0,
		fmt.Sprintf(`%v values must be an object with value names as keys.`, gt),
	)
	if err != nil {
		return values, err
	}

	for valueName, valueConfig := range valueMap {
		err := invariant(
			valueConfig != nil,
			fmt.Sprintf(`%v.%v must refer to an object with a "value" key `+
				`representing an internal value but got: %v.`, gt, valueName, valueConfig),
		)
		if err != nil {
			return values, err
		}
		err = assertValidName(valueName)
		if err != nil {
			return values, err
		}
		value := &EnumValueDefinition{
			Name:              valueName,
			Value:             valueConfig.Value,
			DeprecationReason: valueConfig.DeprecationReason,
			Description:       valueConfig.Description,
		}
		if value.Value == nil {
			value.Value = valueName
		}
		values = append(values, value)
	}
	return values, nil
}
func (gt *Enum) Values() []*EnumValueDefinition {
	return gt.values
}
func (gt *Enum) Serialize(value interface{}) interface{} {
	if enumValue, ok := gt.getValueLookup()[value]; ok {
		return enumValue.Name
	}
	return nil
}
func (gt *Enum) ParseValue(value interface{}) interface{} {
	valueStr, ok := value.(string)
	if !ok {
		return nil
	}
	if enumValue, ok := gt.getNameLookup()[valueStr]; ok {
		return enumValue.Value
	}
	return nil
}
func (gt *Enum) ParseLiteral(valueAST ast.Value) interface{} {
	if valueAST, ok := valueAST.(*ast.EnumValue); ok {
		if enumValue, ok := gt.getNameLookup()[valueAST.Value]; ok {
			return enumValue.Value
		}
	}
	return nil
}
func (gt *Enum) Name() string {
	return gt.PrivateName
}
func (gt *Enum) Description() string {
	return gt.PrivateDescription
}
func (gt *Enum) String() string {
	return gt.PrivateName
}
func (gt *Enum) Error() error {
	return gt.err
}
func (gt *Enum) getValueLookup() map[interface{}]*EnumValueDefinition {
	if len(gt.valuesLookup) > 0 {
		return gt.valuesLookup
	}
	valuesLookup := map[interface{}]*EnumValueDefinition{}
	for _, value := range gt.Values() {
		valuesLookup[value.Value] = value
	}
	gt.valuesLookup = valuesLookup
	return gt.valuesLookup
}

func (gt *Enum) getNameLookup() map[string]*EnumValueDefinition {
	if len(gt.nameLookup) > 0 {
		return gt.nameLookup
	}
	nameLookup := map[string]*EnumValueDefinition{}
	for _, value := range gt.Values() {
		nameLookup[value.Name] = value
	}
	gt.nameLookup = nameLookup
	return gt.nameLookup
}

// InputObject Type Definition
//
// An input object defines a structured collection of fields which may be
// supplied to a field argument.
//
// Using `NonNull` will ensure that a value must be provided by the query
//
// Example:
//
//     var GeoPoint = new InputObject({
//       name: 'GeoPoint',
//       fields: {
//         lat: { type: new NonNull(Float) },
//         lon: { type: new NonNull(Float) },
//         alt: { type: Float, defaultValue: 0 },
//       }
//     });
type InputObject struct {
	PrivateName        string `json:"name"`
	PrivateDescription string `json:"description"`

	typeConfig InputObjectConfig
	fields     InputObjectFieldMap

	err error
}
type InputObjectFieldConfig struct {
	Type         Input       `json:"type"`
	DefaultValue interface{} `json:"defaultValue"`
	Description  string      `json:"description"`
}
type InputObjectField struct {
	PrivateName        string      `json:"name"`
	Type               Input       `json:"type"`
	DefaultValue       interface{} `json:"defaultValue"`
	PrivateDescription string      `json:"description"`
}

func (st *InputObjectField) Name() string {
	return st.PrivateName
}
func (st *InputObjectField) Description() string {
	return st.PrivateDescription

}
func (st *InputObjectField) String() string {
	return st.PrivateName
}
func (st *InputObjectField) Error() error {
	return nil
}

type InputObjectConfigFieldMap map[string]*InputObjectFieldConfig
type InputObjectFieldMap map[string]*InputObjectField
type InputObjectConfigFieldMapThunk func() InputObjectConfigFieldMap
type InputObjectConfig struct {
	Name        string      `json:"name"`
	Fields      interface{} `json:"fields"`
	Description string      `json:"description"`
}

func NewInputObject(config InputObjectConfig) *InputObject {
	gt := &InputObject{}
	err := invariant(config.Name != "", "Type must be named.")
	if err != nil {
		gt.err = err
		return gt
	}

	gt.PrivateName = config.Name
	gt.PrivateDescription = config.Description
	gt.typeConfig = config
	gt.fields = gt.defineFieldMap()
	return gt
}

func (gt *InputObject) defineFieldMap() InputObjectFieldMap {
	var fieldMap InputObjectConfigFieldMap
	switch gt.typeConfig.Fields.(type) {
	case InputObjectConfigFieldMap:
		fieldMap = gt.typeConfig.Fields.(InputObjectConfigFieldMap)
	case InputObjectConfigFieldMapThunk:
		fieldMap = gt.typeConfig.Fields.(InputObjectConfigFieldMapThunk)()
	}
	resultFieldMap := InputObjectFieldMap{}

	err := invariant(
		len(fieldMap) > 0,
		fmt.Sprintf(`%v fields must be an object with field names as keys or a function which return such an object.`, gt),
	)
	if err != nil {
		gt.err = err
		return resultFieldMap
	}

	for fieldName, fieldConfig := range fieldMap {
		if fieldConfig == nil {
			continue
		}
		err := assertValidName(fieldName)
		if err != nil {
			continue
		}
		err = invariant(
			fieldConfig.Type != nil,
			fmt.Sprintf(`%v.%v field type must be Input Type but got: %v.`, gt, fieldName, fieldConfig.Type),
		)
		if err != nil {
			gt.err = err
			return resultFieldMap
		}
		field := &InputObjectField{}
		field.PrivateName = fieldName
		field.Type = fieldConfig.Type
		field.PrivateDescription = fieldConfig.Description
		field.DefaultValue = fieldConfig.DefaultValue
		resultFieldMap[fieldName] = field
	}
	return resultFieldMap
}
func (gt *InputObject) Fields() InputObjectFieldMap {
	return gt.fields
}
func (gt *InputObject) Name() string {
	return gt.PrivateName
}
func (gt *InputObject) Description() string {
	return gt.PrivateDescription
}
func (gt *InputObject) String() string {
	return gt.PrivateName
}
func (gt *InputObject) Error() error {
	return gt.err
}

// List Modifier
//
// A list is a kind of type marker, a wrapping type which points to another
// type. Lists are often created within the context of defining the fields of
// an object type.
//
// Example:
//
//     var PersonType = new Object({
//       name: 'Person',
//       fields: () => ({
//         parents: { type: new List(Person) },
//         children: { type: new List(Person) },
//       })
//     })
//
type List struct {
	OfType Type `json:"ofType"`

	err error
}

func NewList(ofType Type) *List {
	gl := &List{}

	err := invariant(ofType != nil, fmt.Sprintf(`Can only create List of a Type but got: %v.`, ofType))
	if err != nil {
		gl.err = err
		return gl
	}

	gl.OfType = ofType
	return gl
}
func (gl *List) Name() string {
	return fmt.Sprintf("%v", gl.OfType)
}
func (gl *List) Description() string {
	return ""
}
func (gl *List) String() string {
	if gl.OfType != nil {
		return fmt.Sprintf("[%v]", gl.OfType)
	}
	return ""
}
func (gl *List) Error() error {
	return gl.err
}

// NonNull Modifier
//
// A non-null is a kind of type marker, a wrapping type which points to another
// type. Non-null types enforce that their values are never null and can ensure
// an error is raised if this ever occurs during a request. It is useful for
// fields which you can make a strong guarantee on non-nullability, for example
// usually the id field of a database row will never be null.
//
// Example:
//
//     var RowType = new Object({
//       name: 'Row',
//       fields: () => ({
//         id: { type: new NonNull(String) },
//       })
//     })
//
// Note: the enforcement of non-nullability occurs within the executor.
type NonNull struct {
	PrivateName string `json:"name"` // added to conform with introspection for NonNull.Name = nil
	OfType      Type   `json:"ofType"`

	err error
}

func NewNonNull(ofType Type) *NonNull {
	gl := &NonNull{}

	_, isOfTypeNonNull := ofType.(*NonNull)
	err := invariant(ofType != nil && !isOfTypeNonNull, fmt.Sprintf(`Can only create NonNull of a Nullable Type but got: %v.`, ofType))
	if err != nil {
		gl.err = err
		return gl
	}
	gl.OfType = ofType
	return gl
}
func (gl *NonNull) Name() string {
	return fmt.Sprintf("%v!", gl.OfType)
}
func (gl *NonNull) Description() string {
	return ""
}
func (gl *NonNull) String() string {
	if gl.OfType != nil {
		return gl.Name()
	}
	return ""
}
func (gl *NonNull) Error() error {
	return gl.err
}

var NameRegExp, _ = regexp.Compile("^[_a-zA-Z][_a-zA-Z0-9]*$")

func assertValidName(name string) error {
	return invariant(
		NameRegExp.MatchString(name),
		fmt.Sprintf(`Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but "%v" does not.`, name),
	)
}
