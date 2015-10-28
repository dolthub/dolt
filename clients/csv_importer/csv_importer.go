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
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	p = flag.Uint("p", 512, "parallelism")
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

	dsFlags := dataset.NewFlags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	path := flag.Arg(0)
	if ds == nil || path == "" {
		flag.Usage()
		return
	}

	res, err := os.Open(path)
	d.Exp.NoError(err)
	defer res.Close()

	r := csv.NewReader(res)
	r.FieldsPerRecord = 0 // Let first row determine the number of fields.

	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	recordChan := make(chan valuesWithIndex, 4096)
	refChan := make(chan refIndex, 4096)

	wg := sync.WaitGroup{}
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

		close(recordChan)
	}()

	rowsToNoms := func() {
		for row := range recordChan {
			m := types.NewMap()
			for i, v := range row.values {
				m = m.Set(types.NewString(keys[i]), types.NewString(v))
			}

			r := types.NewRef(types.WriteValue(m, ds.Store()))
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
