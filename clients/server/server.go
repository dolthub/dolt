package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset/mgmt"
	"github.com/attic-labs/noms/ref"
)

var (
	port = flag.String("port", "8000", "")
)

type server struct {
	cs chunks.ChunkStore
}

func (s server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	switch r.URL.Path[1:] {
	case "root":
		w.Header().Add("content-type", "text/plain")
		fmt.Fprintf(w, "%v", s.cs.Root().String())
	case "get":
		if refs, ok := r.URL.Query()["ref"]; ok {
			s.handleGetRef(w, refs[0])
		} else {
			http.Error(w, "Missing query param ref", http.StatusBadRequest)
		}
	case "dataset":
		if ids, ok := r.URL.Query()["id"]; ok {
			s.handleGetDataset(w, ids[0])
		} else {
			http.Error(w, "Missing query param id", http.StatusBadRequest)
		}
	default:
		http.Error(w, fmt.Sprintf("Unrecognized: %v", r.URL.Path[1:]), http.StatusBadRequest)
	}
}

func (s server) handleGetRef(w http.ResponseWriter, hashString string) {
	ref, err := ref.Parse(hashString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Parse error: %v", err), http.StatusBadRequest)
		return
	}

	reader, err := s.cs.Get(ref)
	if err != nil {
		// TODO: Maybe we should not expose the internal path?
		http.Error(w, fmt.Sprintf("Fetch error: %v", err), http.StatusNotFound)
		return
	}

	if reader == nil {
		http.Error(w, fmt.Sprintf("No such ref: %v", hashString), http.StatusNotFound)
		return
	}

	w.Header().Add("content-type", "application/octet-stream")
	w.Header().Add("cache-control", "max-age=31536000") // 1 year
	io.Copy(w, reader)
}

func (s server) handleGetDataset(w http.ResponseWriter, id string) {
	dataStore := datas.NewDataStore(s.cs, s.cs.(chunks.RootTracker))
	dataset := mgmt.GetDatasetHeads(mgmt.GetDatasets(dataStore), id)
	if dataset == nil {
		http.Error(w, fmt.Sprintf("Dataset not found: %s", id), http.StatusNotFound)
		return
	}
	w.Header().Add("content-type", "text/plain")
	fmt.Fprintf(w, "%s", dataset.Ref())
}

func main() {
	flags := chunks.NewFlags("")
	flag.Parse()

	cs := flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	http.HandleFunc("/", server{cs}.handle)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
