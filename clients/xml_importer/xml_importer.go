package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/clbanning/mxj"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	noIO        = flag.Bool("benchmark", false, "Run in 'benchmark' mode, without file-IO")
	customUsage = func() {
		fmtString := `%s walks the given directory, looking for .xml files. When it finds one, the entity inside is parsed into nested Noms maps/lists and committed to the dataset indicated on the command line.`
		fmt.Fprintf(os.Stderr, fmtString, os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\nUsage: %s [options] <path/to/root/directory>\n", os.Args[0])
		flag.PrintDefaults()
	}
)

type fileIndex struct {
	path  string
	index int
}

type refIndex struct {
	ref   ref.Ref
	index int
}

type refIndexList []refIndex

func (a refIndexList) Len() int           { return len(a) }
func (a refIndexList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a refIndexList) Less(i, j int) bool { return a[i].index < a[j].index }

func main() {
	err := d.Try(func() {
		dsFlags := dataset.NewFlags()
		flag.Usage = customUsage
		flag.Parse()
		ds := dsFlags.CreateDataset()
		dir := flag.Arg(0)
		if ds == nil || dir == "" {
			flag.Usage()
			return
		}
		defer ds.Close()

		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		cpuCount := runtime.NumCPU()
		runtime.GOMAXPROCS(cpuCount)

		filesChan := make(chan fileIndex, 1024)
		refsChan := make(chan refIndex, 1024)

		getFilePaths := func() {
			index := 0
			err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				d.Exp.NoError(err, "Cannot traverse directories")
				if !info.IsDir() && filepath.Ext(path) == ".xml" {
					filesChan <- fileIndex{path, index}
					index++
				}

				return nil
			})
			d.Exp.NoError(err)
			close(filesChan)
		}

		wg := sync.WaitGroup{}
		importXml := func() {
			expectedType := util.NewMapOfStringToValue()
			for f := range filesChan {
				file, err := os.Open(f.path)
				d.Exp.NoError(err, "Error getting XML")

				xmlObject, err := mxj.NewMapXmlReader(file)
				d.Exp.NoError(err, "Error decoding XML")
				object := xmlObject.Old()
				file.Close()

				nomsObj := util.NomsValueFromDecodedJSON(object)
				d.Chk.IsType(expectedType, nomsObj)
				r := ref.Ref{}

				if !*noIO {
					r = types.WriteValue(nomsObj, ds.Store())
				}

				refsChan <- refIndex{r, f.index}
			}

			wg.Done()
		}

		go getFilePaths()
		for i := 0; i < cpuCount*8; i++ {
			wg.Add(1)
			go importXml()
		}
		go func() {
			wg.Wait()
			close(refsChan) // done converting xml to noms
		}()

		refList := refIndexList{}
		for r := range refsChan {
			refList = append(refList, r)
		}
		sort.Sort(refList)

		refs := make(util.ListOfRefOfMapOfStringToValueDef, len(refList))
		for idx, r := range refList {
			refs[idx] = r.ref
		}

		if !*noIO {
			_, ok := ds.Commit(refs.New())
			d.Exp.True(ok, "Could not commit due to conflicting edit")
		}

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
