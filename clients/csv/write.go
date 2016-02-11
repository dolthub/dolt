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
func Write(l types.List, sd types.StructDesc, comma rune, output io.Writer) {
	d.Exp.Equal(types.StructKind, sd.Kind(), "Did not find Struct: %s", sd.Describe())
	fieldNames := getFieldNamesFromStruct(sd)

	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma

	d.Exp.NoError(csvWriter.Write(fieldNames), "Failed to write header %v", fieldNames)
	record := make([]string, len(fieldNames))
	l.IterAll(func(v types.Value, index uint64) {
		for i, f := range fieldNames {
			record[i] = fmt.Sprintf("%s", v.(types.Struct).Get(f))
		}
		d.Exp.NoError(csvWriter.Write(record), "Failed to write record %v", record)
	})

	csvWriter.Flush()
	d.Exp.NoError(csvWriter.Error(), "error flushing csv")
}

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	for _, f := range structDesc.Fields {
		d.Exp.Equal(true, types.IsPrimitiveKind(f.T.Desc.Kind()),
			"Non-primitive CSV export not supported:", f.T.Desc.Describe())
		fieldNames = append(fieldNames, f.Name)
	}
	return
}
