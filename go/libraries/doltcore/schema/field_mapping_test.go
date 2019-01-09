package schema

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"reflect"
	"testing"
)

var fieldsA = []*Field{
	NewField("a", types.StringKind, true),
	NewField("b", types.StringKind, true),
	NewField("c", types.StringKind, true),
}

var fieldsB = []*Field{
	NewField("a", types.StringKind, true),
	NewField("b", types.StringKind, true),
}

var fieldsC = []*Field{
	NewField("key", types.UUIDKind, true),
	NewField("value", types.StringKind, true),
}

var fieldsD = []*Field{
	NewField("key", types.StringKind, true),
	NewField("value", types.StringKind, true),
}

var schemaA = NewSchema(fieldsA)
var schemaB = NewSchema(fieldsB)
var schemaC = NewSchema(fieldsC)
var schemaCNoPK = NewSchema(fieldsC)
var schemaD = NewSchema(fieldsD)

func init() {
	schemaC.AddConstraint(NewConstraint(PrimaryKey, []int{0}))
}

func TestFieldMapping(t *testing.T) {
	tests := []struct {
		mappingJSON string
		inSch       *Schema
		outSch      *Schema
		expectErr   bool
		expected    []int
		identity    bool
	}{
		{"", schemaA, schemaA, false, []int{0, 1, 2}, true},
		{"", schemaA, schemaB, false, []int{0, 1}, false},
		{"", schemaB, schemaA, false, []int{0, 1, -1}, false},
		{"", schemaA, schemaC, true, nil, false},
		{`{"invalid_json": }`, schemaA, schemaC, true, nil, false},
		{`{"b": "value"}`, schemaA, schemaC, true, nil, false},
		{`{"c": "key", "b": "value"}`, schemaA, schemaC, false, []int{2, 1}, false},
		{`{"a": "key", "b": "value"}`, schemaA, schemaC, false, []int{0, 1}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaC, false, []int{0, 1}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaCNoPK, false, []int{0, 1}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaD, false, []int{0, 1}, true},
	}

	for _, test := range tests {
		fs := filesys.NewInMemFS([]string{"/"}, nil, "/")

		mappingFile := ""
		if test.mappingJSON != "" {
			mappingFile = "mapping.json"
			fs.WriteFile(mappingFile, []byte(test.mappingJSON))
		}

		var mapping *FieldMapping
		var err error
		if mappingFile != "" {
			mapping, err = MappingFromFile(mappingFile, fs, test.inSch, test.outSch)
		} else {
			mapping, err = NewInferredMapping(test.inSch, test.outSch)
		}

		if (err != nil) != test.expectErr {
			if test.expectErr {
				t.Fatal("Expected an error that didn't come.", err)
			} else {
				t.Fatal("Unexpected error creating mapping.", err)
			}
		}

		if !test.expectErr {
			if !reflect.DeepEqual(mapping.DestToSrc, test.expected) {
				t.Error("Mapping does not match expected.  Expected:", test.expected, "Actual:", mapping.DestToSrc)
			}

			//if test.identity != mapping.IsIdentityMapping() {
			//	t.Error("identity expected", test.identity, "actual:", !test.identity)
			//}
		}
	}
}
