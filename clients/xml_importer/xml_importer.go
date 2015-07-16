package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/clbanning/mxj"
)

var (
	customUsage = func() {
		fmtString := `%s walks the given directory, looking for .xml files. When it finds one, the entity inside is parsed into nested Noms maps/lists and committed to the dataset indicated on the command line.`
		fmt.Fprintf(os.Stderr, fmtString, os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\nUsage: %s [options] <path/to/root/directory>\n", os.Args[0])
		flag.PrintDefaults()
	}
)

func main() {
	dsFlags := dataset.Flags()
	flag.Usage = customUsage
	flag.Parse()
	ds := dsFlags.CreateDataset()
	dir := flag.Arg(0)
	if ds == nil || dir == "" {
		flag.Usage()
		return
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
		xmlObject, err := mxj.NewMapXmlReader(file)
		if err != nil {
			log.Fatalln("Error decoding XML: ", err)
		}
		objects = append(objects, xmlObject.Old())
		return nil
	})

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			ds.Roots().NomsValue()).SetValue(
			util.NomsValueFromDecodedJSON(objects))))

}
