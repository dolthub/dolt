// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// ValueToListAndElemDesc ensures that v is a types.List of structs, pulls the types.StructDesc that describes the elements of v out of vr, and returns the List and related StructDesc.
func ValueToListAndElemDesc(v types.Value, vr types.ValueReader) (types.List, types.StructDesc) {
	d.Exp.True(types.ListKind == v.Type().Kind())

	t := v.Type().Desc.(types.CompoundDesc).ElemTypes[0]
	d.Exp.True(types.StructKind == t.Kind())
	return v.(types.List), t.Desc.(types.StructDesc)
}

// Write takes a types.List l of structs (described by sd) and writes it to output as comma-delineated values.
func Write(l types.List, sd types.StructDesc, comma rune, output io.Writer) {
	fieldNames := getFieldNamesFromStruct(sd)

	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma

	d.Exp.NoError(csvWriter.Write(fieldNames), "Failed to write header %v", fieldNames)
	record := make([]string, len(fieldNames))
	l.IterAll(func(v types.Value, index uint64) {
		for i, f := range fieldNames {
			record[i] = fmt.Sprintf("%v", v.(types.Struct).Get(f))
		}
		d.Exp.NoError(csvWriter.Write(record), "Failed to write record %v", record)
	})

	csvWriter.Flush()
	d.Exp.NoError(csvWriter.Error(), "error flushing csv")
}

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	structDesc.IterFields(func(name string, t *types.Type) {
		d.Exp.True(types.IsPrimitiveKind(t.Kind()))
		fieldNames = append(fieldNames, name)
	})
	return
}
