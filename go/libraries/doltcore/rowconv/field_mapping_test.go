package rowconv

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"reflect"
	"testing"
)

var fieldsA, _ = schema.NewColCollection(
	schema.NewColumn("a", 0, types.StringKind, true),
	schema.NewColumn("b", 1, types.StringKind, false),
	schema.NewColumn("c", 2, types.StringKind, false))

var fieldsB, _ = schema.NewColCollection(
	schema.NewColumn("a", 0, types.StringKind, true),
	schema.NewColumn("b", 1, types.StringKind, false))

var fieldsC, _ = schema.NewColCollection(
	schema.NewColumn("key", 3, types.UUIDKind, true),
	schema.NewColumn("value", 4, types.StringKind, false))

var fieldsCNoPK, _ = schema.NewColCollection(
	schema.NewColumn("key", 3, types.UUIDKind, true),
	schema.NewColumn("value", 4, types.StringKind, false))

var fieldsD, _ = schema.NewColCollection(
	schema.NewColumn("key", 3, types.StringKind, true),
	schema.NewColumn("value", 4, types.StringKind, false))

var schemaA = schema.SchemaFromCols(fieldsA)
var schemaB = schema.SchemaFromCols(fieldsB)
var schemaC = schema.SchemaFromCols(fieldsC)
var schemaCNoPK = schema.SchemaFromCols(fieldsCNoPK)
var schemaD = schema.SchemaFromCols(fieldsD)

func TestFieldMapping(t *testing.T) {
	tests := []struct {
		mappingJSON string
		inSch       schema.Schema
		outSch      schema.Schema
		expectErr   bool
		expected    map[uint64]uint64
		identity    bool
	}{
		{"", schemaA, schemaA, false, map[uint64]uint64{0: 0, 1: 1, 2: 2}, true},
		{"", schemaA, schemaB, false, map[uint64]uint64{0: 0, 1: 1}, false},
		{"", schemaB, schemaA, false, map[uint64]uint64{0: 0, 1: 1}, false},
		{"", schemaA, schemaC, true, nil, false},
		{`{"invalid_json": }`, schemaA, schemaC, true, nil, false},
		{`{"b": "value"}`, schemaA, schemaC, false, map[uint64]uint64{1: 4}, false},
		{`{"c": "key", "b": "value"}`, schemaA, schemaC, false, map[uint64]uint64{2: 3, 1: 4}, false},
		{`{"a": "key", "b": "value"}`, schemaA, schemaC, false, map[uint64]uint64{0: 3, 1: 4}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaC, false, map[uint64]uint64{0: 3, 1: 4}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaCNoPK, false, map[uint64]uint64{0: 3, 1: 4}, false},
		{`{"a": "key", "b": "value"}`, schemaB, schemaD, false, map[uint64]uint64{0: 3, 1: 4}, true},
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
			mapping, err = TagMapping(test.inSch, test.outSch)
		}

		if (err != nil) != test.expectErr {
			if test.expectErr {
				t.Fatal("Expected an error that didn't come.")
			} else {
				t.Fatal("Unexpected error creating mapping.", err)
			}
		}

		if !test.expectErr {
			if !reflect.DeepEqual(mapping.SrcToDest, test.expected) {
				t.Error("Mapping does not match expected.  Expected:", test.expected, "Actual:", mapping.SrcToDest)
			}

			//if test.identity != mapping.IsIdentityMapping() {
			//	t.Error("identity expected", test.identity, "actual:", !test.identity)
			//}
		}
	}
}
