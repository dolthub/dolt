package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Store().Close()

	url := flag.Arg(0)
	if ds == nil || url == "" {
		flag.Usage()
		return
	}

	res, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error fetching %s: %+v\n", url, err)
	} else if res.StatusCode != 200 {
		log.Fatalf("Error fetching %s: %s\n", url, res.Status)
	}
	defer res.Body.Close()

	var jsonObject interface{}
	err = json.NewDecoder(res.Body).Decode(&jsonObject)
	if err != nil {
		log.Fatalln("Error decoding JSON: ", err)
	}

	_, err = ds.Commit(util.NomsValueFromDecodedJSON(jsonObject))
	d.Exp.NoError(err)
}
