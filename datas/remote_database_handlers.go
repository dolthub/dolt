// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

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
	"github.com/attic-labs/noms/hash"
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
	hashes := hash.HashSlice{}
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
			} else {
				bpe = append(bpe, c.Hash())
			}
			// If a previous Enqueue() errored, we still need to drain chunkChan
			// TODO: what about having DeserializeToChan take a 'done' channel to stop it?
			hashes = append(hashes, c.Hash())
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
		http.Error(w, fmt.Sprintf("Error: %v\nChunks in payload: %v", err, hashes), http.StatusBadRequest)
		return
	}
}

// Contents of the returned io.Reader are gzipped.
func buildWriteValueRequest(serializedChunks io.Reader, hints map[hash.Hash]struct{}) io.Reader {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	serializeHints(gw, hints)
	d.Chk.NoError(gw.Close())
	return io.MultiReader(body, serializedChunks)
}

func HandleGetRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		hashes := extractHashes(req)

		w.Header().Add("Content-Type", "application/octet-stream")
		writer := respWriter(req, w)
		defer writer.Close()

		sz := chunks.NewSerializer(writer)
		for _, h := range hashes {
			c := cs.Get(h)
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

func extractHashes(req *http.Request) hash.HashSlice {
	err := req.ParseForm()
	d.Exp.NoError(err)
	hashStrs := req.PostForm["ref"]
	d.Exp.True(len(hashStrs) > 0)

	hashes := make(hash.HashSlice, len(hashStrs))
	for idx, refStr := range hashStrs {
		hashes[idx] = hash.Parse(refStr)
	}
	return hashes
}

func buildHashesRequest(hashes map[hash.Hash]struct{}) io.Reader {
	values := &url.Values{}
	for r := range hashes {
		values.Add("ref", r.String())
	}
	return strings.NewReader(values.Encode())
}

func HandleHasRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		hashes := extractHashes(req)

		w.Header().Add("Content-Type", "text/plain")
		writer := respWriter(req, w)
		defer writer.Close()

		for _, h := range hashes {
			fmt.Fprintf(writer, "%s %t\n", h, cs.Has(h))
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
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
		last := hash.Parse(tokens[0])
		tokens = params["current"]
		d.Exp.Len(tokens, 1)
		current := hash.Parse(tokens[0])

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
