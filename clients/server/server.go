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
	chunks.ChunkStore
}

func (s server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	switch r.URL.Path[1:] {
	case "root":
		cs := s.ChunkStore
		w.Header().Add("content-type", "text/plain")
		fmt.Fprintf(w, "%v", cs.Root().String())
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
	cs := s.ChunkStore
	ref, err := ref.Parse(hashString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Parse error: %v", err), http.StatusBadRequest)
		return
	}

	reader, err := cs.Get(ref)
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
	cs := s.ChunkStore
	rootDataStore := datas.NewDataStore(cs, cs.(chunks.RootTracker))
	dataset := mgmt.GetDatasetRoot(mgmt.GetDatasets(rootDataStore), id)
	if dataset == nil {
		http.Error(w, fmt.Sprintf("Dataset not found: %s", id), http.StatusNotFound)
		return
	}
	w.Header().Add("content-type", "text/plain")
	fmt.Fprintf(w, "%s", dataset.Ref())
}

func main() {
	flags := chunks.NewFlags()
	flag.Parse()

	cs := flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	http.HandleFunc("/", cs.(server).handle)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
