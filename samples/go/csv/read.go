// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// StringToKind maps names of valid NomsKinds (e.g. Bool, Number, etc) to their associated types.NomsKind
var StringToKind = func(kindMap map[types.NomsKind]string) map[string]types.NomsKind {
	m := map[string]types.NomsKind{}
	for k, v := range kindMap {
		m[v] = k
	}
	return m
}(types.KindToString)

// StringsToKinds looks up each element of strs in the StringToKind map and returns a slice of answers
func StringsToKinds(strs []string) KindSlice {
	kinds := make(KindSlice, len(strs))
	for i, str := range strs {
		k, ok := StringToKind[str]
		if !ok {
			d.Panic("StringToKind[%s] failed", str)
		}
		kinds[i] = k
	}
	return kinds
}

// KindsToStrings looks up each element of kinds in the types.KindToString map and returns a slice of answers
func KindsToStrings(kinds KindSlice) []string {
	strs := make([]string, len(kinds))
	for i, k := range kinds {
		strs[i] = k.String()
	}
	return strs
}

//EscapeStructFieldFromCSV removes special characters and replaces spaces with camelCasing (camel case turns to camelCase)
func EscapeStructFieldFromCSV(input string) string {
	if types.IsValidStructFieldName(input) {
		return input
	}
	return types.CamelCaseFieldName(input)
}

// MakeStructTemplateFromHeaders creates a struct type from the headers using |kinds| as the type of each field. If |kinds| is empty, default to strings.
func MakeStructTemplateFromHeaders(headers []string, structName string, kinds KindSlice) (temp types.StructTemplate, fieldOrder []int, kindMap []types.NomsKind) {
	useStringType := len(kinds) == 0
	d.PanicIfFalse(useStringType || len(headers) == len(kinds))

	fieldMap := make(map[string]types.NomsKind, len(headers))
	origOrder := make(map[string]int, len(headers))
	fieldNames := make(sort.StringSlice, len(headers))

	for i, key := range headers {
		fn := EscapeStructFieldFromCSV(key)
		origOrder[fn] = i
		kind := types.StringKind
		if !useStringType {
			kind = kinds[i]
		}
		_, ok := fieldMap[fn]
		if ok {
			d.Panic(`Duplicate field name "%s"`, key)
		}
		fieldMap[fn] = kind
		fieldNames[i] = fn
	}

	sort.Sort(fieldNames)

	kindMap = make([]types.NomsKind, len(fieldMap))
	fieldOrder = make([]int, len(fieldMap))

	for i, fn := range fieldNames {
		kindMap[i] = fieldMap[fn]
		fieldOrder[origOrder[fn]] = i
	}

	temp = types.MakeStructTemplate(structName, fieldNames)
	return
}

// ReadToList takes a CSV reader and reads data into a typed List of structs.
// Each row gets read into a struct named structName, described by headers. If
// the original data contained headers it is expected that the input reader has
// already read those and are pointing at the first data row.
// If kinds is non-empty, it will be used to type the fields in the generated
// structs; otherwise, they will be left as string-fields.
// In addition to the list, ReadToList returns the typeDef of the structs in the
// list.
func ReadToList(r *csv.Reader, structName string, headers []string, kinds KindSlice, vrw types.ValueReadWriter, limit uint64) (l types.List) {
	temp, fieldOrder, kindMap := MakeStructTemplateFromHeaders(headers, structName, kinds)
	valueChan := make(chan types.Value, 128) // TODO: Make this a function param?
	listChan := types.NewStreamingList(vrw, valueChan)

	cnt := uint64(0)
	for {
		row, err := r.Read()
		if cnt >= limit || err == io.EOF {
			close(valueChan)
			break
		} else if err != nil {
			panic(err)
		}
		cnt++

		fields := readFieldsFromRow(row, headers, fieldOrder, kindMap)
		valueChan <- temp.NewStruct(fields)
	}

	return <-listChan
}

type column struct {
	ch        chan types.Value
	list      <-chan types.List
	zeroValue types.Value
	hdr       string
}

// ReadToColumnar takes a CSV reader and reads data from each column into a
// separate list. Values from columns in each successive row are appended to the
// column-specific lists whose type is described by headers. Finally, a new
// "Columnar" struct is created that consists of one field for each column and
// each field contains a list of values.
// If the original data contained headers it is expected that the input reader
// has already read those and are pointing at the first data row.
// If kinds is non-empty, it will be used to type the fields in the generated
// structs; otherwise, they will be left as string-fields.
// In addition to the list, ReadToList returns the typeDef of the structs in the
// list.
func ReadToColumnar(r *csv.Reader, structName string, headers []string, kinds KindSlice, vrw types.ValueReadWriter, limit uint64) (s types.Struct) {
	valueChan := make(chan types.Value, 128) // TODO: Make this a function param?
	cols := []column{}
	fieldOrder := []int{}
	for i, hdr := range headers {
		ch := make(chan types.Value, 1024)
		cols = append(cols, column{
			ch:   ch,
			list: types.NewStreamingList(vrw, ch),
			hdr:  hdr,
		})
		fieldOrder = append(fieldOrder, i)
	}

	cnt := uint64(0)
	for {
		row, err := r.Read()
		if cnt >= limit || err == io.EOF {
			close(valueChan)
			break
		} else if err != nil {
			panic(err)
		}
		cnt++

		fields := readFieldsFromRow(row, headers, fieldOrder, kinds)
		for i, v := range fields {
			cols[i].ch <- v
		}
	}

	sd := types.StructData{}
	for _, col := range cols {
		close(col.ch)
		r := vrw.WriteValue(<-col.list)
		sd[col.hdr] = r
	}
	return types.NewStruct("Columnar", sd)
}

// getFieldIndexByHeaderName takes the collection of headers and the name to search for and returns the index of name within the headers or -1 if not found
func getFieldIndexByHeaderName(headers []string, name string) int {
	for i, header := range headers {
		if header == name {
			return i
		}
	}
	return -1
}

// getPkIndices takes collection of primary keys as strings and determines if they are integers, if so then use those ints as the indices, otherwise it looks up the strings in the headers to find the indices; returning the collection of int indices representing the primary keys maintaining the order of strPks to the return collection
func getPkIndices(strPks []string, headers []string) []int {
	result := make([]int, len(strPks))
	for i, pk := range strPks {
		pkIdx, ok := strconv.Atoi(pk)
		if ok == nil {
			result[i] = pkIdx
		} else {
			result[i] = getFieldIndexByHeaderName(headers, pk)
		}
		if result[i] < 0 {
			d.Chk.Fail(fmt.Sprintf("Invalid pk: %v", pk))
		}
	}
	return result
}

func readFieldsFromRow(row []string, headers []string, fieldOrder []int, kindMap []types.NomsKind) types.ValueSlice {
	fields := make(types.ValueSlice, len(headers))
	for i, v := range row {
		if i < len(headers) {
			fieldOrigIndex := fieldOrder[i]
			val, err := StringToValue(v, kindMap[fieldOrigIndex])
			if err != nil {
				d.Chk.Fail(fmt.Sprintf("Error parsing value for column '%s': %s", headers[i], err))
			}
			fields[fieldOrigIndex] = val
		}
	}
	return fields
}

// primaryKeyValuesFromFields extracts the values of the primaryKey fields into
// array. The values are in the user-specified order. This function returns 2
// objects:
//    1) a ValueSlice containing the first n-1 keys.
//    2) a single Value which will be used as the key in the leaf map created by
//       GraphBuilder
func primaryKeyValuesFromFields(fields types.ValueSlice, fieldOrder, pkIndices []int) (types.ValueSlice, types.Value) {
	numPrimaryKeys := len(pkIndices)

	if numPrimaryKeys == 1 {
		return nil, fields[fieldOrder[pkIndices[0]]]
	}

	keys := make(types.ValueSlice, numPrimaryKeys-1)
	var value types.Value
	for i, idx := range pkIndices {
		k := fields[fieldOrder[idx]]
		if i < numPrimaryKeys-1 {
			keys[i] = k
		} else {
			value = k
		}
	}
	return keys, value
}

// ReadToMap takes a CSV reader and reads data into a typed Map of structs. Each
// row gets read into a struct named structName, described by headers. If the
// original data contained headers it is expected that the input reader has
// already read those and are pointing at the first data row.
// If kinds is non-empty, it will be used to type the fields in the generated
// structs; otherwise, they will be left as string-fields.
func ReadToMap(r *csv.Reader, structName string, headersRaw []string, primaryKeys []string, kinds KindSlice, vrw types.ValueReadWriter, limit uint64) types.Map {
	temp, fieldOrder, kindMap := MakeStructTemplateFromHeaders(headersRaw, structName, kinds)
	pkIndices := getPkIndices(primaryKeys, headersRaw)
	d.Chk.True(len(pkIndices) >= 1, "No primary key defined when reading into map")
	gb := types.NewGraphBuilder(vrw, types.MapKind)

	cnt := uint64(0)
	for {
		row, err := r.Read()
		if cnt >= limit || err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		cnt++

		fields := readFieldsFromRow(row, headersRaw, fieldOrder, kindMap)
		graphKeys, mapKey := primaryKeyValuesFromFields(fields, fieldOrder, pkIndices)
		st := temp.NewStruct(fields)
		gb.MapSet(graphKeys, mapKey, st)
	}
	return gb.Build().(types.Map)
}
