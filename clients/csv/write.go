package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	for _, f := range structDesc.Fields {
		d.Exp.Equal(true, types.IsPrimitiveKind(f.T.Desc.Kind()),
			"Non-primitive CSV export not supported:", f.T.Desc.Describe())
		fieldNames = append(fieldNames, f.Name)
	}
	return
}

func datasetToHeaderAndList(ds *dataset.Dataset) (fieldNames []string, nomsList types.List) {
	v := ds.Head().Value()
	d.Exp.Equal(types.ListKind, v.Type().Desc.Kind(),
		"Dataset must be List<>, found:", v.Type().Desc.Describe())

	t := v.Type().Desc.(types.CompoundDesc).ElemTypes[0]
	d.Exp.Equal(types.RefKind, t.Desc.Kind(),
		"List<> must be of Ref, found:", t.Desc.Describe())

	u := t.Desc.(types.CompoundDesc).ElemTypes[0]
	d.Exp.Equal(types.UnresolvedKind, u.Desc.Kind(),
		"Ref must be UnresolvedKind, found:", u.Desc.Describe())

	pkg := types.ReadPackage(u.PackageRef(), ds.Store())
	d.Exp.Equal(types.PackageKind, pkg.Type().Desc.Kind(),
		"Failed to read package:", pkg.Type().Desc.Describe())

	structDesc := pkg.Types()[u.Ordinal()].Desc
	d.Exp.Equal(types.StructKind, structDesc.Kind(),
		"Did not find Struct:", structDesc.Describe())

	fieldNames = getFieldNamesFromStruct(structDesc.(types.StructDesc))
	nomsList = v.(types.List)
	return
}

func Write(ds *dataset.Dataset, comma rune, concurrency int, output io.Writer) {
	fieldNames, nomsList := datasetToHeaderAndList(ds)

	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma

	records := make([][]string, nomsList.Len()+1)
	records[0] = fieldNames // Write header

	nomsList.IterAllP(concurrency, func(v types.Value, index uint64) {
		for _, f := range fieldNames {
			records[index+1] = append(
				records[index+1],
				fmt.Sprintf("%s", v.(types.Ref).TargetValue(ds.Store()).(types.Struct).
					Get(f)))
		}
	})

	csvWriter.WriteAll(records)
	err := csvWriter.Error()
	d.Exp.Equal(nil, err, "error flushing csv:", err)
}
