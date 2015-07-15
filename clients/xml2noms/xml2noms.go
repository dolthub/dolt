package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/clbanning/mxj"
)

func main() {
	dsFlags := dataset.Flags()
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
