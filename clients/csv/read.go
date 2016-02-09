package csv

import (
	"encoding/csv"
	"io"
	"log"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// MakeStructTypeFromHeader creates a struct type by reading the first row of the csv.Reader.
func MakeStructTypeFromHeader(r *csv.Reader, structName string) (typeRef types.Type, typeDef types.Type) {
	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	fields := make([]types.Field, 0, len(keys))
	for _, key := range keys {
		fields = append(fields, types.Field{
			Name: key,
			T:    types.MakePrimitiveType(types.StringKind),
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
func Read(res io.Reader, structName, header string, comma rune, cs chunks.ChunkStore) (l types.List, typeRef types.Type, typeDef types.Type) {
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

	typeRef, typeDef = MakeStructTypeFromHeader(r, structName)

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
			fields[structFields[i].Name] = types.NewString(v)
		}
		valueChan <- types.NewStruct(typeRef, typeDef, fields)
	}

	return <-listChan, typeRef, typeDef
}
