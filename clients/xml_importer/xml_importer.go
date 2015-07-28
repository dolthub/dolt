package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/clbanning/mxj"
)

var (
	noIO        = flag.Bool("benchmark", false, "Run in 'benchmark' mode, without file-IO")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile  = flag.String("memprofile", "", "write memory profile to this file")
	customUsage = func() {
		fmtString := `%s walks the given directory, looking for .xml files. When it finds one, the entity inside is parsed into nested Noms maps/lists and committed to the dataset indicated on the command line.`
		fmt.Fprintf(os.Stderr, fmtString, os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\nUsage: %s [options] <path/to/root/directory>\n", os.Args[0])
		flag.PrintDefaults()
	}
)

func main() {
	dsFlags := dataset.NewFlags()
	flag.Usage = customUsage
	flag.Parse()
	ds := dsFlags.CreateDataset()
	dir := flag.Arg(0)
	if ds == nil || dir == "" {
		flag.Usage()
		return
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var objects []interface{}
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalln("Cannot traverse directories: ", err)
		}
		if info.IsDir() || filepath.Ext(path) != ".xml" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			log.Fatalln("Error getting XML: ", err)
		}
		defer file.Close()
		xmlObject, err := mxj.NewMapXmlReader(file)
		if err != nil {
			log.Fatalln("Error decoding XML: ", err)
		}
		objects = append(objects, xmlObject.Old())
		return nil
	})

	noms := util.NomsValueFromDecodedJSON(objects)

	if !*noIO {
		ds.Commit(datas.NewSetOfCommit().Insert(
			datas.NewCommit().SetParents(
				ds.Heads().NomsValue()).SetValue(noms)))
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}
}
