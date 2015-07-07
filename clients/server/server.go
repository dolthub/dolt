package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

var (
	port *string = flag.String("port", "8000", "")
	cs   chunks.ChunkStore
)

func handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	switch r.URL.Path[1:] {
	case "root":
		w.Header().Add("content-type", "text/plain")
		fmt.Fprintf(w, "%v", cs.Root().String())
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

func main() {
	flags := chunks.NewFlags()
	flag.Parse()

	cs = flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	http.HandleFunc("/", handler)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
