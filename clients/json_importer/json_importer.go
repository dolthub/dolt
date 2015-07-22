package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/attic-labs/noms/clients/go"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
)

func main() {
	dsFlags := dataset.Flags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}

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

	var jsonObject interface{}
	err = json.NewDecoder(res.Body).Decode(&jsonObject)
	if err != nil {
		log.Fatalln("Error decoding JSON: ", err)
	}

	commits := ds.Heads()

	value := util.NomsValueFromDecodedJSON(jsonObject)

	ds.Commit(datas.NewCommitSet().Insert(
		datas.NewCommit().SetParents(
			commits.NomsValue()).SetValue(
			value)))

}
