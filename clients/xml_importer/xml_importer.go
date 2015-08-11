package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/clbanning/mxj"
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

		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		list := types.NewList()

		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			d.Exp.NoError(err, "Cannot traverse directories")
			if info.IsDir() || filepath.Ext(path) != ".xml" {
				return nil
			}
			file, err := os.Open(path)
			d.Exp.NoError(err, "Error getting XML")
			defer file.Close()

			xmlObject, err := mxj.NewMapXmlReader(file)
			d.Exp.NoError(err, "Error decoding XML")
			object := xmlObject.Old()

			nomsObj := util.NomsValueFromDecodedJSON(object)
			if *noIO {
				return nil
			}

			ref, err := types.WriteValue(nomsObj, ds)
			d.Exp.NoError(err, "Failed to write noms value")

			list = list.Append(types.Ref{R: ref})
			return nil
		})
		d.Exp.NoError(err)

		if !*noIO {
			ds.Commit(datas.NewSetOfCommit().Insert(
				datas.NewCommit().SetParents(
					ds.Heads().NomsValue()).SetValue(list)))
		}

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
