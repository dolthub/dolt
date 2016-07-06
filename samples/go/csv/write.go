// Copyright 2016 Attic Labs, Inc. All rights reserved.
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
	d.PanicIfTrue(types.ListKind != v.Type().Kind(), "Expected ListKind, found %s", types.KindToString[v.Type().Kind()])

	t := v.Type().Desc.(types.CompoundDesc).ElemTypes[0]
	d.PanicIfTrue(types.StructKind != t.Kind(), "Expected StructKind, found %s", types.KindToString[v.Type().Kind()])
	return v.(types.List), t.Desc.(types.StructDesc)
}

// Write takes a types.List l of structs (described by sd) and writes it to output as comma-delineated values.
func Write(l types.List, sd types.StructDesc, comma rune, output io.Writer) {
	structChan := make(chan types.Struct, 1024)
	go func() {
		l.IterAll(func(v types.Value, index uint64) {
			structChan <- v.(types.Struct)
		})
		close(structChan)
	}()

	fieldNames := getFieldNamesFromStruct(sd)
	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma
	d.PanicIfTrue(csvWriter.Write(fieldNames) != nil, "Failed to write header %v", fieldNames)
	record := make([]string, len(fieldNames))
	for s := range structChan {
		for i, f := range fieldNames {
			record[i] = fmt.Sprintf("%v", s.Get(f))
		}
		d.PanicIfTrue(csvWriter.Write(record) != nil, "Failed to write record %v", record)
	}

	csvWriter.Flush()
	d.PanicIfTrue(csvWriter.Error() != nil, "error flushing csv")
}

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	structDesc.IterFields(func(name string, t *types.Type) {
		d.PanicIfTrue(!types.IsPrimitiveKind(t.Kind()), "Expected primitive kind, found %s", types.KindToString[t.Kind()])
		fieldNames = append(fieldNames, name)
	})
	return
}
