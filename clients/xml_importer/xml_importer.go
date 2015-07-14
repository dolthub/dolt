package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/clbanning/mxj"
)

func main() {
	dsFlags := dataset.Flags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	url := flag.Arg(0)
	if ds == nil || url == "" {
		flag.Usage()
		return
	}

	res, err := http.Get(url)
	defer res.Body.Close()
	if err != nil {
		log.Fatalf("Error fetching %s: %+v\n", url, err)
	} else if res.StatusCode != 200 {
		log.Fatalf("Error fetching %s: %s\n", url, res.Status)
	}

	xmlObject, err := mxj.NewMapXmlReader(res.Body)
	if err != nil {
		log.Fatalln("Error decoding XML: ", err)
	}

	roots := ds.Roots()

	value := util.NomsValueFromDecodedJSON(xmlObject.Old())

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			value)))

}
