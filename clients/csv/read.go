package csv

import (
	"encoding/csv"
	"io"
	"log"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var StringToKind = func(kindMap map[types.NomsKind]string) map[string]types.NomsKind {
	m := map[string]types.NomsKind{}
	for k, v := range kindMap {
		m[v] = k
	}
	return m
}(types.KindToString)

func StringsToKinds(strs []string) []types.NomsKind {
	kinds := make([]types.NomsKind, len(strs), len(strs))
	for i, str := range strs {
		k, ok := StringToKind[str]
		d.Exp.True(ok)
		kinds[i] = k
	}

	return kinds
}

func KindsToStrings(kinds []types.NomsKind) []string {
	strs := make([]string, len(kinds), len(kinds))
	for i, k := range kinds {
		strs[i] = types.KindToString[k]
	}

	return strs
}

func ReportValidFieldTypes(res io.Reader, header string) ([]string, [][]types.NomsKind) {
	var input io.Reader
	if len(header) == 0 {
		input = res
	} else {
		input = io.MultiReader(
			strings.NewReader(header+"\n"),
			res)
	}

	r := csv.NewReader(input)

	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	options := NewSchemaOptions(len(keys))
	rowChan := make(chan []string)
	go func() {
		for row := range rowChan {
			options.Test(row)
		}
	}()

	for {
		row, err := r.Read()
		if err == io.EOF {
			close(rowChan)
			break
		} else if err != nil {
			log.Fatalln("Error decoding CSV: ", err)
		}

		rowChan <- row
	}

	return keys, options.ValidKinds()
}

// MakeStructTypeFromHeader creates a struct type by reading the first row of the csv.Reader using |kinds| as the type of each field. If |kinds| is empty, default to strings.
func MakeStructTypeFromHeader(r *csv.Reader, structName string, kinds []types.NomsKind) (typeRef types.Type, typeDef types.Type) {
	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	useStringType := len(kinds) == 0
	d.Chk.True(useStringType || len(keys) == len(kinds))
	fields := make([]types.Field, 0, len(keys))
	d.Chk.True(useStringType || len(kinds) == len(keys))
	for i, key := range keys {
		kind := types.StringKind
		if !useStringType {
			kind = kinds[i]
		}
		fields = append(fields, types.Field{
			Name: key,
			T:    types.MakePrimitiveType(kind),
			// TODO(misha): Think about whether we need fields to be optional.
			Optional: false,
		})
	}

	typeDef = types.MakeStructType(structName, fields, types.Choices{})
	pkg := types.NewPackage([]types.Type{typeDef}, []ref.Ref{})
	pkgRef := types.RegisterPackage(&pkg)
	typeRef = types.MakeType(pkgRef, 0)

	return
}

// Read takes comma-delineated data from res and parsed into a typed List of structs. Each row gets parsed into a struct named structName, optionally described by header. If header is empty, the first line of the file is used to guess the form of the struct into which rows are parsed.
// In addition to the list, Read returns the typeRef for the structs in the list, and last the typeDef of the structs.
func Read(res io.Reader, structName, header string, kinds []types.NomsKind, comma rune, cs chunks.ChunkStore) (l types.List, typeRef types.Type, typeDef types.Type) {
	var input io.Reader
	if len(header) == 0 {
		input = res
	} else {
		input = io.MultiReader(
			strings.NewReader(header+"\n"),
			res)
	}

	r := csv.NewReader(input)
	r.Comma = comma
	r.FieldsPerRecord = 0 // Let first row determine the number of fields.

	typeRef, typeDef = MakeStructTypeFromHeader(r, structName, kinds)

	valueChan := make(chan types.Value)
	listType := types.MakeCompoundType(types.ListKind, typeRef)
	listChan := types.NewStreamingTypedList(listType, cs, valueChan)

	structFields := typeDef.Desc.(types.StructDesc).Fields

	for {
		row, err := r.Read()
		if err == io.EOF {
			close(valueChan)
			break
		} else if err != nil {
			log.Fatalln("Error decoding CSV: ", err)
		}

		fields := make(map[string]types.Value)
		for i, v := range row {
			f := structFields[i]
			fields[f.Name] = StringToType(v, f.T.Kind())
		}
		valueChan <- types.NewStruct(typeRef, typeDef, fields)
	}

	return <-listChan, typeRef, typeDef
}
