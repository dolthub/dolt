package main

import (
	"encoding/csv"
	"flag"
	"io"
	"log"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func main() {
	flags := chunks.NewFlags()
	flag.Parse()
	cs := flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	url := flag.Arg(0)
	if url == "" {
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

	r := csv.NewReader(res.Body)
	r.FieldsPerRecord = 0 // Let first row determine the number of fields.

	keys, err := r.Read()
	if err != nil {
		log.Fatalln("Error decoding CSV: ", err)
	}

	value := types.NewList()
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("Error decoding CSV: ", err)
		}

		m := types.NewMap()
		for i, v := range row {
			m = m.Set(types.NewString(keys[i]), types.NewString(v))
		}
		value = value.Append(m)
	}

	ds := datas.NewDataStore(cs)
	roots := ds.Roots()

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			value)))
}
