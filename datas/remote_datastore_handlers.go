package datas

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type URLParams interface {
	ByName(string) string
}

type Handler func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore)

// HandlePostRefs puts a bunch of chunks into cs without doing any validation. This is bad and shall be done away with once we fix BUG 822.
func HandlePostRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}

		chunks.Deserialize(reader, cs, nil)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleWriteValue(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}
		cvr := newCachingValueStore(&naiveHintedChunkStore{cs})
		// Prime cvr with the refs embedded in the 'hinted' chunks.
		for _, hint := range deserializeHints(reader) {
			cvr.ReadValue(hint)
		}

		var orderedChunks []chunks.Chunk
		chunkChan := make(chan chunks.Chunk, 16)
		go chunks.DeserializeToChan(reader, chunkChan)
		var r ref.Ref
		for c := range chunkChan {
			r = c.Ref()
			if cvr.isPresent(r) {
				continue
			}
			v := types.DecodeChunk(c, &cvr)
			d.Exp.NotNil(v, "Chunk with hash %s failed to decode", r)
			cvr.checkChunksInCache(v)
			cvr.set(r, presentChunk(v.Type()))
			orderedChunks = append(orderedChunks, c)
		}
		/*err := */ cs.PutMany(orderedChunks)
		// TODO communicate backpressure from err
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

// Contents of the returned io.Reader are gzipped.
func buildWriteValueRequest(chnx []chunks.Chunk, hints map[ref.Ref]struct{}) io.Reader {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	serializeHints(gw, hints)
	sz := chunks.NewSerializer(gw)
	for _, chunk := range chnx {
		sz.Put(chunk)
	}
	sz.Close()
	gw.Close()
	return body
}

func HandleGetRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		err := req.ParseForm()
		d.Exp.NoError(err)
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
			c := cs.Get(r)
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

func buildGetRefsRequest(refs map[ref.Ref]struct{}) io.Reader {
	values := &url.Values{}
	for r := range refs {
		values.Add("ref", r.String())
	}
	return strings.NewReader(values.Encode())
}

func HandleRootGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("GET", req.Method)

		rootRef := rt.Root()
		fmt.Fprintf(w, "%v", rootRef.String())
		w.Header().Add("content-type", "text/plain")
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func HandleRootPost(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		params := req.URL.Query()
		tokens := params["last"]
		d.Exp.Len(tokens, 1)
		last := ref.Parse(tokens[0])
		tokens = params["current"]
		d.Exp.Len(tokens, 1)
		current := ref.Parse(tokens[0])

		if !rt.UpdateRoot(current, last) {
			w.WriteHeader(http.StatusConflict)
			return
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}
