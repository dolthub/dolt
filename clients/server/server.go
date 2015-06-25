package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	port    *string = flag.String("port", "8000", "")
	cs      chunks.ChunkStore
	tracker chunks.RootTracker
)

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	switch r.URL.Path[1:] {
	case "root":
		w.Header().Add("content-type", "text/plain")
		fmt.Fprintf(w, "%v", tracker.Root().String())
	case "get":
		hashString := r.URL.Query()["ref"][0]
		ref, err := ref.Parse(hashString)
		if err != nil {
			http.Error(w, fmt.Sprintf("Parse error: %v", err), http.StatusBadRequest)
			return
		}

		reader, err := cs.Get(ref)
		if err != nil {
			http.Error(w, fmt.Sprintf("Fetch error: %v", err), http.StatusNotFound)
			return
		}

		w.Header().Add("content-type", "application/octet-stream")
		w.Header().Add("cache-control", "max-age=31536000") // 1 year
		io.Copy(w, reader)
	default:
		http.Error(w, fmt.Sprintf("Unrecognized: %v", r.URL.Path[1:]), http.StatusBadRequest)
	}
}

func createDummyData() {
	a := types.NewSet(
		types.NewString("foo"),
		types.Int64(34),
		types.Float64(3.4),
		types.NewList(
			types.NewString("bar"),
		),
		types.NewMap(
			types.NewString("foo"), types.NewString("bar"),
			types.NewString("amount"), types.Bool(true),
		),
	)

	Chk.NotNil(cs)
	enc.WriteValue(a, cs)
	tracker.UpdateRoot(a.Ref(), tracker.Root())
}

func main() {
	flags := chunks.NewFlags()
	flag.Parse()

	cs = flags.CreateStore()
	Chk.NotNil(cs)

	// TODO: Shouldn't have to cast here.
	tracker = cs.(chunks.RootTracker)

	createDummyData()

	http.HandleFunc("/", handler)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
