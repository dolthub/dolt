package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/clbanning/mxj"
)

func nomsValueFromDecodedJSON(o interface{}) types.Value {
	switch o := o.(type) {
	case mxj.Map:
		return util.NomsValueFromDecodedJSON(o.Old())
	default:
		return util.NomsValueFromDecodedJSON(o)
	}
}

func main() {
	datasetDataStoreFlags := dataset.DatasetDataFlags()
	flag.Parse()
	ds := datasetDataStoreFlags.CreateStore()
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

	value := nomsValueFromDecodedJSON(xmlObject)

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			value)))

}
