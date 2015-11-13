package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	p       = flag.Uint("p", 512, "parallelism")
	dsFlags = dataset.NewFlags()
	// Actually the delimiter uses runes, which can be multiple charcter long.
	// https://blog.golang.org/strings
	delimiter = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header    = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name      = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
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

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Println("Usage: csv_importer [options] file\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	if flag.NArg() != 1 {
		fmt.Printf("Expected exactly one parameter (path) after flags, but you have %d. Maybe you put a flag after the path?\n", flag.NArg())
		flag.Usage()
		return
	}

	path := flag.Arg(0)
	if ds == nil || path == "" {
		flag.Usage()
		return
	}

	res, err := os.Open(path)
	d.Exp.NoError(err)
	defer res.Close()

	var input io.Reader
	if len(*header) == 0 {
		input = res
	} else {
		input = io.MultiReader(
			strings.NewReader(*header+"\n"),
			res)
	}

	comma, err := getDelimiter()
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		return
	}
	r := csv.NewReader(input)
	r.Comma = comma
	r.FieldsPerRecord = 0 // Let first row determine the number of fields.

	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	fields := make([]types.Field, 0, len(keys))
	for _, key := range keys {
		fields = append(fields, types.Field{
			Name: key,
			T:    types.MakePrimitiveTypeRef(types.StringKind),
			// TODO(misha): Think about whether we need fields to be optional.
			Optional: false,
		})
	}

	typeDef := types.MakeStructTypeRef(*name, fields, types.Choices{})
	pkg := types.NewPackage([]types.Type{typeDef}, []ref.Ref{})
	pkgRef := types.RegisterPackage(&pkg)
	typeRef := types.MakeTypeRef(pkgRef, 0)

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

	rowsToNoms := func() {
		for row := range recordChan {
			fields := make(map[string]types.Value)
			for i, v := range row.values {
				fields[keys[i]] = types.NewString(v)
			}
			newStruct := types.NewStruct(typeRef, typeDef, fields)
			r := types.NewRef(types.WriteValue(newStruct, ds.Store()))
			refChan <- refIndex{r, row.index}
		}
	}

	for i := uint(0); i < *p; i++ {
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

	value := types.NewList(refs...)
	_, ok := ds.Commit(value)
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}

// Returns the rune contained in *delimiter or an error.
func getDelimiter() (rune, error) {
	dlimLen := len(*delimiter)
	if dlimLen == 0 {
		return 0, fmt.Errorf("delimiter flag must contain exactly one character (rune), not an empty string")
	}

	d, runeSize := utf8.DecodeRuneInString(*delimiter)
	if d == utf8.RuneError {
		return 0, fmt.Errorf("Invalid utf8 string in delimiter flag: %s", *delimiter)
	}
	if dlimLen != runeSize {
		return 0, fmt.Errorf("delimiter flag is too long. It must contain exactly one character (rune), but instead it is: %s", *delimiter)
	}
	return d, nil
}
