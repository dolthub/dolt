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

func getElemDesc(s types.Collection, index int) types.StructDesc {
	t := s.Type().Desc.(types.CompoundDesc).ElemTypes[index]
	if types.StructKind != t.Kind() {
		d.Panic("Expected StructKind, found %s", types.KindToString[t.Type().Kind()])
	}
	return t.Desc.(types.StructDesc)
}

// GetListElemDesc ensures that l is a types.List of structs, pulls the types.StructDesc that describes the elements of l out of vr, and returns the StructDesc.
func GetListElemDesc(l types.List, vr types.ValueReader) types.StructDesc {
	return getElemDesc(l, 0)
}

// GetMapElemDesc ensures that m is a types.Map of structs, pulls the types.StructDesc that describes the elements of m out of vr, and returns the StructDesc.
// If m is a nested types.Map of types.Map, then GetMapElemDesc will descend the levels of the enclosed types.Maps to get to a types.Struct
func GetMapElemDesc(m types.Map, vr types.ValueReader) types.StructDesc {
	t := m.Type().Desc.(types.CompoundDesc).ElemTypes[1]
	if types.StructKind == t.Kind() {
		return t.Desc.(types.StructDesc)
	} else if types.MapKind == t.Kind() {
		_, v := m.First()
		return GetMapElemDesc(v.(types.Map), vr)
	}
	panic(fmt.Sprintf("Expected StructKind or MapKind, found %s", types.KindToString[t.Type().Kind()]))
}

func writeValuesFromChan(structChan chan types.Struct, sd types.StructDesc, comma rune, output io.Writer) {
	fieldNames := getFieldNamesFromStruct(sd)
	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = comma
	if csvWriter.Write(fieldNames) != nil {
		d.Panic("Failed to write header %v", fieldNames)
	}
	record := make([]string, len(fieldNames))
	for s := range structChan {
		for i, f := range fieldNames {
			record[i] = fmt.Sprintf("%v", s.Get(f))
		}
		if csvWriter.Write(record) != nil {
			d.Panic("Failed to write record %v", record)
		}
	}

	csvWriter.Flush()
	if csvWriter.Error() != nil {
		d.Panic("error flushing csv")
	}
}

// Write takes a types.List l of structs (described by sd) and writes it to output as comma-delineated values.
func WriteList(l types.List, sd types.StructDesc, comma rune, output io.Writer) {
	structChan := make(chan types.Struct, 1024)
	go func() {
		l.IterAll(func(v types.Value, index uint64) {
			structChan <- v.(types.Struct)
		})
		close(structChan)
	}()
	writeValuesFromChan(structChan, sd, comma, output)
}

func sendMapValuesToChan(m types.Map, structChan chan<- types.Struct) {
	m.IterAll(func(k, v types.Value) {
		if subMap, ok := v.(types.Map); ok {
			sendMapValuesToChan(subMap, structChan)
		} else {
			structChan <- v.(types.Struct)
		}
	})
}

// Write takes a types.Map m of structs (described by sd) and writes it to output as comma-delineated values.
func WriteMap(m types.Map, sd types.StructDesc, comma rune, output io.Writer) {
	structChan := make(chan types.Struct, 1024)
	go func() {
		sendMapValuesToChan(m, structChan)
		close(structChan)
	}()
	writeValuesFromChan(structChan, sd, comma, output)
}

func getFieldNamesFromStruct(structDesc types.StructDesc) (fieldNames []string) {
	structDesc.IterFields(func(name string, t *types.Type) {
		if !types.IsPrimitiveKind(t.Kind()) {
			d.Panic("Expected primitive kind, found %s", types.KindToString[t.Kind()])
		}
		fieldNames = append(fieldNames, name)
	})
	return
}
