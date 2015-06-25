package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	csFlags = chunks.NewFlags()
	pkg     = flag.String("pkg", "", "blah")
	refStr  = flag.String("ref", "", "blah")
	outDir  = flag.String("out", "", "blah")
)

func main() {
	flag.Parse()
	cs := csFlags.CreateStore()
	if cs == nil || *refStr == "" {
		flag.Usage()
		return
	}
	val := enc.MustReadValue(ref.MustParse(*refStr), cs)
	writeStr(`package %s

import (
	"github.com/attic-labs/noms/types"
)

`, *pkg)
	writeType(val.(types.Map))
}

func writeType(val types.Map) {
	typ := val.Get(types.NewString("$type")).(types.String).String()
	switch typ {
	case "noms.StructDef":
		writeStruct(val)
	default:
		Chk.Fail("Unknown $type: %s", typ)
	}
}

func writeStruct(val types.Map) {
	structName := val.Get(types.NewString("$name")).(types.String).String()
	writeStr(`type %s struct {
	types.Value
}
`, structName)
}

func writeStr(str string, vals ...interface{}) {
	os.Stdout.WriteString(fmt.Sprintf(str, vals...))
}
