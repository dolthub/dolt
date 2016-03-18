package datas

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type URLParams interface {
	ByName(string) string
}

type Handler func(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore)

func HandleRef(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("GET", req.Method)
		r := ref.Parse(ps.ByName("ref"))

		all := req.URL.Query().Get("all")
		if all == "true" {
			handleGetReachable(w, req, r, ds)
			return
		}
		chunk := ds.transitionalChunkStore().Get(r)
		if chunk.IsEmpty() {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_, err := io.Copy(w, bytes.NewReader(chunk.Data()))
		d.Chk.NoError(err)
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Header().Add("Cache-Control", "max-age=31536000") // 1 year
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func handleGetReachable(w http.ResponseWriter, req *http.Request, r ref.Ref, ds DataStore) {
	excludeRef := ref.Ref{}
	exclude := req.URL.Query().Get("exclude")
	if exclude != "" {
		excludeRef = ref.Parse(exclude)
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	writer := w.(io.Writer)
	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Add("Content-Encoding", "gzip")
		gw := gzip.NewWriter(w)
		defer gw.Close()
		writer = gw
	}

	sz := newDataSink(chunks.NewSerializer(writer))
	ds.CopyReachableChunksP(r, excludeRef, sz, 512)
	sz.Close()
}

type localDataSink struct {
	cs chunks.ChunkSink
}

func newDataSink(cs chunks.ChunkSink) DataSink {
	return &localDataSink{cs}
}

func (lds *localDataSink) transitionalChunkSink() chunks.ChunkSink {
	return lds.cs
}

func (lds *localDataSink) WriteValue(v types.Value) ref.Ref {
	chunk := types.EncodeValue(v, lds)
	lds.cs.Put(chunk)
	return chunk.Ref()
}

func (lds *localDataSink) Close() error {
	return lds.cs.Close()
}

func HandlePostRefs(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}

		chunks.Deserialize(reader, ds.transitionalChunkStore(), nil)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleGetHasRefs(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		refs := make([]ref.Ref, len(refStrs))
		for idx, refStr := range refStrs {
			refs[idx] = ref.Parse(refStr)
		}

		w.Header().Add("Content-Type", "text/plain")
		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		sz := chunks.NewSerializer(writer)
		for _, r := range refs {
			has := ds.transitionalChunkStore().Has(r)
			fmt.Fprintf(writer, "%s %t\n", r, has)
		}
		sz.Close()
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleGetRefs(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		refs := make([]ref.Ref, len(refStrs))
		for idx, refStr := range refStrs {
			refs[idx] = ref.Parse(refStr)
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		sz := chunks.NewSerializer(writer)
		for _, r := range refs {
			c := ds.transitionalChunkStore().Get(r)
			if !c.IsEmpty() {
				sz.Put(c)
			}
		}
		sz.Close()
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleRootGet(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("GET", req.Method)

		rootRef := ds.transitionalChunkStore().Root()
		fmt.Fprintf(w, "%v", rootRef.String())
		w.Header().Add("content-type", "text/plain")
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleRootPost(w http.ResponseWriter, req *http.Request, ps URLParams, ds DataStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		params := req.URL.Query()
		tokens := params["last"]
		d.Exp.Len(tokens, 1)
		last := ref.Parse(tokens[0])
		tokens = params["current"]
		d.Exp.Len(tokens, 1)
		current := ref.Parse(tokens[0])

		if !ds.transitionalChunkStore().UpdateRoot(current, last) {
			w.WriteHeader(http.StatusConflict)
			return
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}
