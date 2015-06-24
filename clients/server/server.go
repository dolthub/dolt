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

// TODO(rafael): CORS header shouldn't allow access from anywhere?
func handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path[1:] {
	case "root":
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("content-type", "application/octet-stream")
		fmt.Fprintf(w, "%v", tracker.Root().String())
	case "get":
		hashString := r.URL.Query()["ref"][0]
		ref, _ := ref.Parse(hashString)
		reader, _ := cs.Get(ref)
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("content-type", "application/octet-stream")
		io.Copy(w, reader)
	default:
		fmt.Fprintf(w, "Unrecognized: %v", r.URL.Path[1:])
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

	// TODO(rafael): Shouldn't have to cast here.
	tracker = cs.(chunks.RootTracker)

	createDummyData()

	http.HandleFunc("/", handler)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
