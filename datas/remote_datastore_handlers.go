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

		reader := bodyReader(req)
		defer reader.Close()
		chunks.Deserialize(reader, cs, nil)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func bodyReader(req *http.Request) (reader io.ReadCloser) {
	reader = req.Body
	if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.Exp.NoError(err)
		reader = gr
	}
	return
}

func respWriter(req *http.Request, w http.ResponseWriter) (writer io.WriteCloser) {
	writer = wc{w.(io.Writer)}
	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Add("Content-Encoding", "gzip")
		gw := gzip.NewWriter(w)
		writer = gw
	}
	return
}

type wc struct {
	io.Writer
}

func (wc wc) Close() error {
	return nil
}

func HandleWriteValue(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	hashes := ref.RefSlice{}
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		reader := bodyReader(req)
		defer reader.Close()
		vbs := types.NewValidatingBatchingSink(cs)
		vbs.Prepare(deserializeHints(reader))

		chunkChan := make(chan chunks.Chunk, 16)
		go chunks.DeserializeToChan(reader, chunkChan)
		var bpe chunks.BackpressureError
		for c := range chunkChan {
			if bpe == nil {
				bpe = vbs.Enqueue(c)
			}
			// If a previous Enqueue() errored, we still need to drain chunkChan
			// TODO: what about having DeserializeToChan take a 'done' channel to stop it?
			hashes = append(hashes, c.Ref())
		}
		if bpe == nil {
			bpe = vbs.Flush()
		}
		if bpe != nil {
			w.WriteHeader(httpStatusTooManyRequests)
			w.Header().Add("Content-Type", "application/octet-stream")
			writer := respWriter(req, w)
			defer writer.Close()
			serializeHashes(writer, bpe.AsHashes())
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v\nSaw hashes %v", err, hashes), http.StatusBadRequest)
		return
	}
}

// Contents of the returned io.Reader are gzipped.
func buildWriteValueRequest(serializedChunks io.Reader, hints map[ref.Ref]struct{}) io.Reader {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	serializeHints(gw, hints)
	d.Chk.NoError(gw.Close())
	return io.MultiReader(body, serializedChunks)
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
		writer := respWriter(req, w)
		defer writer.Close()

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
