package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

// ValueToListAndElemDesc ensures that v is a types.List of structs, pulls the types.StructDesc that describes the elements of v out of cs, and returns the List and related StructDesc.
func ValueToListAndElemDesc(v types.Value, cs chunks.ChunkSource) (types.List, types.StructDesc) {
	d.Exp.Equal(types.ListKind, v.Type().Desc.Kind(),
		"Dataset must be List<>, found: %s", v.Type().Desc.Describe())

	u := v.Type().Desc.(types.CompoundDesc).ElemTypes[0]
	d.Exp.Equal(types.UnresolvedKind, u.Desc.Kind(),
		"List<> must be UnresolvedKind, found: %s", u.Desc.Describe())

	pkg := types.ReadPackage(u.PackageRef(), cs)
	d.Exp.Equal(types.PackageKind, pkg.Type().Desc.Kind(),
		"Failed to read package: %s", pkg.Type().Desc.Describe())

	desc := pkg.Types()[u.Ordinal()].Desc
	d.Exp.Equal(types.StructKind, desc.Kind(), "Did not find Struct: %s", desc.Describe())
	return v.(types.List), desc.(types.StructDesc)
}

// Write takes a types.List l of structs (described by sd) and writes it to output as comma-delineated values.
func Write(l types.List, sd types.StructDesc, comma rune, concurrency int, output io.Writer) {
	d.Exp.Equal(types.StructKind, sd.Kind(), "Did not find Struct: %s", sd.Describe())
	fieldNames := getFieldNamesFromStruct(sd)

	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma

	records := make([][]string, l.Len()+1)
	records[0] = fieldNames // Write header

	l.IterAllP(concurrency, func(v types.Value, index uint64) {
		for _, f := range fieldNames {
			records[index+1] = append(
				records[index+1],
				fmt.Sprintf("%s", v.(types.Struct).Get(f)),
			)
		}
	})

	csvWriter.WriteAll(records)
	err := csvWriter.Error()
	d.Exp.Equal(nil, err, "error flushing csv:", err)
}

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	for _, f := range structDesc.Fields {
		d.Exp.Equal(true, types.IsPrimitiveKind(f.T.Desc.Kind()),
			"Non-primitive CSV export not supported:", f.T.Desc.Describe())
		fieldNames = append(fieldNames, f.Name)
	}
	return
}
