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
		d.Exp.True(ok)
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

// NewCSVReader returns a new csv.Reader that splits on comma and asserts that all rows contain the same number of fields as the first.
func NewCSVReader(res io.Reader, comma rune) *csv.Reader {
	r := csv.NewReader(res)
	r.Comma = comma
	r.FieldsPerRecord = 0 // Let first row determine the number of fields.
	return r
}

// ReportValidFieldTypes takes res, a reader expected to contain CSV data, and an optional header. Excluding the header (assumed to be the first row if no header is given), it returns a slice of types.NomsKind for each column in the data indicating what Noms types could be used to represent that row.
// For example, if all values in a row are negative integers between -127 and 0, the slice for that row would be [types.Int8Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Float32Kind, types.Float64Kind, types.StringKind]. If even one value in the row is a floating point number, however, all the integer kinds would be dropped. All values can be represented as a string, so that option is always provided.
func ReportValidFieldTypes(res io.Reader, header string) ([]string, []KindSlice) {
	var input io.Reader
	if len(header) == 0 {
		input = res
	} else {
		input = io.MultiReader(strings.NewReader(header+"\n"), res)
	}

	r := csv.NewReader(input)
	keys, err := r.Read()
	d.Exp.NoError(err, "Error decoding CSV")

	options := newSchemaOptions(len(keys))
	rowChan := make(chan []string)
	doneChan := make(chan struct{})
	go func() {
		for row := range rowChan {
			options.Test(row)
		}
		doneChan <- struct{}{}
	}()

	for {
		row, err := r.Read()
		if err == io.EOF {
			close(rowChan)
			break
		}
		d.Exp.NoError(err, "Error decoding CSV")

		rowChan <- row
	}
	<-doneChan
	return keys, options.ValidKinds()
}

// MakeStructTypeFromHeader creates a struct type by reading the first row of the csv.Reader using |kinds| as the type of each field. If |kinds| is empty, default to strings.
func MakeStructTypeFromHeader(r *csv.Reader, structName string, kinds KindSlice) (typeRef, typeDef types.Type) {
	keys, err := r.Read()
	d.Exp.NoError(err, "Error decoding CSV")

	useStringType := len(kinds) == 0
	d.Chk.True(useStringType || len(keys) == len(kinds))
	fields := make([]types.Field, len(keys))
	for i, key := range keys {
		kind := types.StringKind
		if !useStringType {
			kind = kinds[i]
		}
		fields[i] = types.Field{
			Name: key,
			T:    types.MakePrimitiveType(kind),
			// TODO(misha): Think about whether we need fields to be optional.
			Optional: false,
		}
	}
	typeDef = types.MakeStructType(structName, fields, types.Choices{})
	pkg := types.NewPackage([]types.Type{typeDef}, []ref.Ref{})
	pkgRef := types.RegisterPackage(&pkg)
	typeRef = types.MakeType(pkgRef, 0)

	return
}

// Read takes comma-delineated data from res and parses it into a typed List of structs. Each row gets parsed into a struct named structName, optionally described by header. If header is empty, the first line of the file is used to guess the form of the struct into which rows are parsed. If kinds is non-empty, it will be used to type the fields in the generated structs; otherwise, they will be left as string-fields.
// In addition to the list, Read returns the typeRef for the structs in the list, and last the typeDef of the structs.
func Read(res io.Reader, structName, header string, kinds KindSlice, comma rune, cs chunks.ChunkStore) (l types.List, typeRef, typeDef types.Type) {
	var input io.Reader
	if len(header) == 0 {
		input = res
	} else {
		input = io.MultiReader(strings.NewReader(header+"\n"), res)
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
