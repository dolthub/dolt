package nomgen

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/types"
)

func TestListSmokeTest(t *testing.T) {
	buf := bytes.Buffer{}
	ng := New(&buf)
	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.ListDef"),
		types.NewString("elem"), types.NewString("int32")))
	ng.WriteGo("test")
}

func TestSetSmokeTest(t *testing.T) {
	buf := bytes.Buffer{}
	ng := New(&buf)
	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), types.NewString("int32")))
	ng.WriteGo("test")
}

func TestMapSmokeTest(t *testing.T) {
	buf := bytes.Buffer{}
	ng := New(&buf)
	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("int32"),
		types.NewString("value"), types.NewString("bool")))

	ng.WriteGo("test")
}

func TestStructSmokeTest(t *testing.T) {
	buf := bytes.Buffer{}
	ng := New(&buf)
	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("MyStruct"),
		types.NewString("key"), types.NewString("int32"),
		types.NewString("value"), types.NewString("bool")))
	ng.WriteGo("test")
}
