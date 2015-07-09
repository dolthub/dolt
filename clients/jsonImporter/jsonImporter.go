package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/types"
)

func nomsValueFromObject(o interface{}) types.Value {
	switch oo := o.(type) {
	case string:
		return types.NewString(oo)
	case bool:
		return types.Bool(oo)
	case float64:
		return types.Float64(oo)
	case nil:
		return nil
	case []interface{}:
		out := types.NewList()
		for _, v := range oo {
			out = out.Append(nomsValueFromObject(v))
		}
		return out
	case map[string]interface{}:
		out := types.NewMap()
		for k, v := range oo {
			nv := nomsValueFromObject(v)
			if nv != nil {
				out = out.Set(types.NewString(k), nv)
			}
		}
		return out
	default:
		fmt.Println(oo, "is of a type I don't know how to handle")
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
	dbg.Chk.NoError(err)

	gameEventsJSON, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	dbg.Chk.NoError(err)

	var events interface{}
	err = json.Unmarshal(gameEventsJSON, &events)
	dbg.Chk.NoError(err)

	ds := datas.NewDataStore(cs)
	roots := ds.Roots()

	gameEvents := nomsValueFromObject(events)

	ds.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(
			roots.NomsValue()).SetValue(
			gameEvents)))

}
