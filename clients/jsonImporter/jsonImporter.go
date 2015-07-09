package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
)

func nomsValueFromObject(o interface{}) types.Value {
	switch o := o.(type) {
	case string:
		return types.NewString(o)
	case bool:
		return types.Bool(o)
	case float64:
		return types.Float64(o)
	case nil:
		return nil
	case []interface{}:
		out := types.NewList()
		for _, v := range o {
			nv := nomsValueFromObject(v)
			if nv != nil {
				out = out.Append(nv)
			}
		}
		return out
	case map[string]interface{}:
		out := types.NewMap()
		for k, v := range o {
			nv := nomsValueFromObject(v)
			if nv != nil {
				out = out.Set(types.NewString(k), nv)
			}
		}
		return out
	default:
		fmt.Println(o, "is of a type I don't know how to handle")
	}
	return nil
}

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

	var jsonObject interface{}
	err = json.NewDecoder(res.Body).Decode(&jsonObject)
	if err != nil {
		log.Fatalln("Error decoding JSON: ", err)
	}

	ds := datas.NewDataStore(cs)
	roots := ds.Roots()

	value := nomsValueFromObject(jsonObject)

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			value)))

}
