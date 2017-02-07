package graphql

import (
	"testing"
)

func TestIsEqualType_SameReferenceAreEqual(t *testing.T) {
	if !isEqualType(String, String) {
		t.Fatalf("Expected same reference to be equal")
	}
}

func TestIsEqualType_IntAndFloatAreNotEqual(t *testing.T) {
	if isEqualType(Int, Float) {
		t.Fatalf("Expected GraphQLInt and GraphQLFloat to not equal")
	}
}

func TestIsEqualType_ListsOfSameTypeAreEqual(t *testing.T) {
	if !isEqualType(NewList(Int), NewList(Int)) {
		t.Fatalf("Expected lists of same type are equal")
	}
}

func TestIsEqualType_ListsAreNotEqualToItem(t *testing.T) {
	if isEqualType(NewList(Int), Int) {
		t.Fatalf("Expected lists are not equal to item")
	}
}

func TestIsEqualType_NonNullOfSameTypeAreEqual(t *testing.T) {
	if !isEqualType(NewNonNull(Int), NewNonNull(Int)) {
		t.Fatalf("Expected non-null of same type are equal")
	}
}
func TestIsEqualType_NonNullIsNotEqualToNullable(t *testing.T) {
	if isEqualType(NewNonNull(Int), Int) {
		t.Fatalf("Expected non-null is not equal to nullable")
	}
}

func testSchemaForIsTypeSubTypeOfTest(t *testing.T, fields Fields) *Schema {
	schema, err := NewSchema(SchemaConfig{
		Query: NewObject(ObjectConfig{
			Name:   "Query",
			Fields: fields,
		}),
	})
	if err != nil {
		t.Fatalf("Invalid schema: %v", err)
	}
	return &schema
}

func TestIsTypeSubTypeOf_SameReferenceIsSubtype(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if !isTypeSubTypeOf(schema, String, String) {
		t.Fatalf("Expected same reference is subtype")
	}
}
func TestIsTypeSubTypeOf_IntIsNotSubtypeOfFloat(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if isTypeSubTypeOf(schema, Int, Float) {
		t.Fatalf("Expected int is not subtype of float")
	}
}
func TestIsTypeSubTypeOf_NonNullIsSubtypeOfNullable(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if !isTypeSubTypeOf(schema, NewNonNull(Int), Int) {
		t.Fatalf("Expected non-null is subtype of nullable")
	}
}
func TestIsTypeSubTypeOf_NullableIsNotSubtypeOfNonNull(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if isTypeSubTypeOf(schema, Int, NewNonNull(Int)) {
		t.Fatalf("Expected nullable is not subtype of non-null")
	}
}
func TestIsTypeSubTypeOf_ItemIsNotSubTypeOfList(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if isTypeSubTypeOf(schema, Int, NewList(Int)) {
		t.Fatalf("Expected item is not subtype of list")
	}
}
func TestIsTypeSubTypeOf_ListIsNotSubtypeOfItem(t *testing.T) {
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: String},
	})
	if isTypeSubTypeOf(schema, NewList(Int), Int) {
		t.Fatalf("Expected list is not subtype of item")
	}
}

func TestIsTypeSubTypeOf_MemberIsSubtypeOfUnion(t *testing.T) {
	memberType := NewObject(ObjectConfig{
		Name: "Object",
		IsTypeOf: func(p IsTypeOfParams) bool {
			return true
		},
		Fields: Fields{
			"field": &Field{Type: String},
		},
	})
	unionType := NewUnion(UnionConfig{
		Name:  "Union",
		Types: []*Object{memberType},
	})
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: unionType},
	})
	if !isTypeSubTypeOf(schema, memberType, unionType) {
		t.Fatalf("Expected member is subtype of union")
	}
}

func TestIsTypeSubTypeOf_ImplementationIsSubtypeOfInterface(t *testing.T) {
	ifaceType := NewInterface(InterfaceConfig{
		Name: "Interface",
		Fields: Fields{
			"field": &Field{Type: String},
		},
	})
	implType := NewObject(ObjectConfig{
		Name: "Object",
		IsTypeOf: func(p IsTypeOfParams) bool {
			return true
		},
		Interfaces: []*Interface{ifaceType},
		Fields: Fields{
			"field": &Field{Type: String},
		},
	})
	schema := testSchemaForIsTypeSubTypeOfTest(t, Fields{
		"field": &Field{Type: implType},
	})
	if !isTypeSubTypeOf(schema, implType, ifaceType) {
		t.Fatalf("Expected implementation is subtype of interface")
	}
}
