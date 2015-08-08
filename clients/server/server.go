package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/dataset/mgmt"
	"github.com/attic-labs/noms/ref"
)

var (
	port   = flag.String("port", "8000", "")
	dsName = flag.String("ds", "", "dataset to serve. If empty, the entire database is served")
)

type server struct {
	ds datas.DataStore
}

func (s server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	switch r.URL.Path[1:] {
	// TODO: Rename "heads"
	case "root":
		w.Header().Add("content-type", "text/plain")
		// Important to use Heads() here, not Root() because Root() refers to root of entire ChunkStore, not root of the datastore. They can differ.
		fmt.Fprintf(w, "%v", s.ds.Heads().Ref())
	case "get":
		if refs, ok := r.URL.Query()["ref"]; ok {
			s.handleGetRef(w, refs[0])
		} else {
			http.Error(w, "Missing query param ref", http.StatusBadRequest)
		}
	case "dataset":
		// TODO: Remove this. BUG 185.
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

	reader, err := s.ds.Get(ref)
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
	dataset := mgmt.GetDatasetHeads(mgmt.GetDatasets(s.ds), id)
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

	dataStore := datas.NewDataStore(cs)
	if *dsName != "" {
		ds := mgmt.GetDatasetHeads(mgmt.GetDatasets(dataStore), *dsName)
		if ds == nil {
			fmt.Println("Dataset not found: ", *dsName)
			return
		}
		dataStore = dataset.NewDataset(dataStore, *dsName).DataStore
	}

	http.HandleFunc("/", server{dataStore}.handle)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}
