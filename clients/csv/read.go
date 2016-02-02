package csv

import (
	"encoding/csv"
	"io"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type valuesWithIndex struct {
	values []string
	index  int
}

type refIndex struct {
	ref   types.Ref
	index int
}

type refIndexList []refIndex

func (a refIndexList) Len() int           { return len(a) }
func (a refIndexList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a refIndexList) Less(i, j int) bool { return a[i].index < a[j].index }

// Creates a struct type by reading the first row of the csv.Reader.
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

func Read(res io.Reader, structName, header string, comma rune, p uint, cs chunks.ChunkStore) (types.List, types.Type, types.Type) {
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

	typeRef, typeDef := MakeStructTypeFromHeader(r, structName)

	recordChan := make(chan valuesWithIndex, 4096)
	refChan := make(chan refIndex, 4096)

	wg := sync.WaitGroup{}
	wg.Add(1)
	index := 0
	go func() {
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln("Error decoding CSV: ", err)
			}

			wg.Add(1)
			recordChan <- valuesWithIndex{row, index}
			index++
		}

		wg.Done()
		close(recordChan)
	}()

	structFields := typeDef.Desc.(types.StructDesc).Fields

	rowsToNoms := func() {
		for row := range recordChan {
			fields := make(map[string]types.Value)
			for i, v := range row.values {
				fields[structFields[i].Name] = types.NewString(v)
			}
			newStruct := types.NewStruct(typeRef, typeDef, fields)
			r := types.NewRef(types.WriteValue(newStruct, cs))
			refChan <- refIndex{r, row.index}
		}
	}

	for i := uint(0); i < p; i++ {
		go rowsToNoms()
	}

	refList := refIndexList{}

	go func() {
		for r := range refChan {
			refList = append(refList, r)
			wg.Done()
		}
	}()

	wg.Wait()
	sort.Sort(refList)

	refs := make([]types.Value, 0, len(refList))
	for _, r := range refList {
		refs = append(refs, r.ref)
	}

	refType := types.MakeCompoundType(types.RefKind, typeRef)
	listType := types.MakeCompoundType(types.ListKind, refType)

	return types.NewTypedList(listType, refs...), typeRef, typeDef
}
