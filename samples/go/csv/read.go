// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"encoding/csv"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// StringToKind maps names of valid NomsKinds (e.g. Bool, Float32, etc) to their associated types.NomsKind
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
		d.PanicIfTrue(!ok, "StringToKind[%s] failed", str)
		kinds[i] = k
	}
	return kinds
}

// KindsToStrings looks up each element of kinds in the types.KindToString map and returns a slice of answers
func KindsToStrings(kinds KindSlice) []string {
	strs := make([]string, len(kinds))
	for i, k := range kinds {
		strs[i] = types.KindToString[k]
	}
	return strs
}

// MakeStructTypeFromHeaders creates a struct type from the headers using |kinds| as the type of each field. If |kinds| is empty, default to strings.
func MakeStructTypeFromHeaders(headers []string, structName string, kinds KindSlice) *types.Type {
	useStringType := len(kinds) == 0
	d.Chk.True(useStringType || len(headers) == len(kinds))
	fields := make(types.TypeMap, len(headers))
	for i, key := range headers {
		kind := types.StringKind
		if !useStringType {
			kind = kinds[i]
		}
		_, ok := fields[key]
		d.PanicIfTrue(ok, `Duplicate field name "%s"`, key)
		fields[key] = types.MakePrimitiveType(kind)
	}
	return types.MakeStructType(structName, fields)
}

// Read takes a CSV reader and reads it into a typed List of structs. Each row gets read into a struct named structName, described by headers. If the original data contained headers it is expected that the input reader has already read those and are pointing at the first data row.
// If kinds is non-empty, it will be used to type the fields in the generated structs; otherwise, they will be left as string-fields.
// In addition to the list, Read returns the typeRef for the structs in the list, and last the typeDef of the structs.
func ReadToList(r *csv.Reader, structName string, headersRaw []string, kinds KindSlice, vrw types.ValueReadWriter) (l types.List, t *types.Type) {
	headers := make([]string, len(headersRaw))
	for i, h := range headersRaw {
		headers[i] = types.EscapeStructField(h)
	}

	t = MakeStructTypeFromHeaders(headers, structName, kinds)
	valueChan := make(chan types.Value, 128) // TODO: Make this a function param?
	listChan := types.NewStreamingList(vrw, valueChan)

	kindMap := make(map[string]types.NomsKind, len(headers))
	t.Desc.(types.StructDesc).IterFields(func(name string, t *types.Type) {
		kindMap[name] = t.Kind()
	})

	for {
		row, err := r.Read()
		if err == io.EOF {
			close(valueChan)
			break
		} else if err != nil {
			panic(err)
		}

		fields := make(map[string]types.Value)
		for i, v := range row {
			if i < len(headers) {
				name := headers[i]
				fields[name] = StringToType(v, kindMap[name])
			}
		}
		valueChan <- types.NewStructWithType(t, fields)
	}

	return <-listChan, t
}

func ReadToMap(r *csv.Reader, headersRaw []string, pkIdx int, kinds KindSlice, vrw types.ValueReadWriter) types.Map {
	headers := make([]string, 0, len(headersRaw)-1)
	for i, h := range headersRaw {
		if i != pkIdx {
			headers = append(headers, types.EscapeStructField(h))
		}
	}

	var pkKind types.NomsKind
	if len(kinds) == 0 {
		pkKind = types.StringKind
	} else {
		pkKind = kinds[pkIdx]
		kinds = append(kinds[:pkIdx], kinds[pkIdx+1:]...)
	}

	t := MakeStructTypeFromHeaders(headers, "", kinds)
	kindMap := make(map[string]types.NomsKind, len(headers))
	t.Desc.(types.StructDesc).IterFields(func(name string, t *types.Type) {
		kindMap[name] = t.Kind()
	})

	mx := types.NewMap().Mx(vrw)
	fields := map[string]types.Value{}
	var pk types.Value
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		fieldIndex := 0
		for x, v := range row {
			if x == pkIdx {
				pk = StringToType(v, pkKind)
			} else if fieldIndex < len(headers) {
				name := headers[fieldIndex]
				fields[name] = StringToType(v, kindMap[name])
				fieldIndex++
			}
		}
		mx = mx.Set(pk, types.NewStructWithType(t, fields))
	}
	return mx.Finish()
}
