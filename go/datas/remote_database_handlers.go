// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/ngql"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/golang/snappy"
)

type URLParams interface {
	ByName(string) string
}

type Handler func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore)

const (
	// NomsVersionHeader is the name of the header that Noms clients and
	// servers must set in every request/response.
	NomsVersionHeader = "x-noms-vers"
	nomsBaseHTML      = "<html><head></head><body><p>Hi. This is a Noms HTTP server.</p><p>To learn more, visit <a href=\"https://github.com/attic-labs/noms\">our GitHub project</a>.</p></body></html>"
	maxGetBatchSize   = 1 << 11 // Limit GetMany() to ~8MB of data
)

var (
	// HandleWriteValue is meant to handle HTTP POST requests to the
	// writeValue/ server endpoint. The payload should be an appropriately-
	// ordered sequence of Chunks to be validated and stored on the server.
	// TODO: Nice comment about what headers it expects/honors, payload
	// format, and error responses.
	HandleWriteValue = createHandler(handleWriteValue, true)

	// HandleGetRefs is meant to handle HTTP POST requests to the getRefs/
	// server endpoint. Given a sequence of Chunk hashes, the server will
	// fetch and return them.
	// TODO: Nice comment about what headers it
	// expects/honors, payload format, and responses.
	HandleGetRefs = createHandler(handleGetRefs, true)

	// HandleGetBlob is a custom endpoint whose sole purpose is to directly
	// fetch the *bytes* contained in a Blob value. It expects a single query
	// param of `h` to be the ref of the Blob.
	// TODO: Support retrieving blob contents via a path.
	HandleGetBlob = createHandler(handleGetBlob, false)

	// HandleWriteValue is meant to handle HTTP POST requests to the hasRefs/
	// server endpoint. Given a sequence of Chunk hashes, the server check for
	// their presence and return a list of true/false responses.
	// TODO: Nice comment about what headers it expects/honors, payload
	// format, and responses.
	HandleHasRefs = createHandler(handleHasRefs, true)

	// HandleRootGet is meant to handle HTTP GET requests to the root/ server
	// endpoint. The server returns the hash of the Root as a string.
	// TODO: Nice comment about what headers it expects/honors, payload
	// format, and responses.
	HandleRootGet = createHandler(handleRootGet, true)

	// HandleWriteValue is meant to handle HTTP POST requests to the root/
	// server endpoint. This is used to update the Root to point to a new
	// Chunk.
	// TODO: Nice comment about what headers it expects/honors, payload
	// format, and error responses.
	HandleRootPost = createHandler(handleRootPost, true)

	// HandleBaseGet is meant to handle HTTP GET requests to the / server
	// endpoint. This is used to give a friendly message to users.
	// TODO: Nice comment about what headers it expects/honors, payload
	// format, and error responses.
	HandleBaseGet = handleBaseGet

	HandleGraphQL = createHandler(handleGraphQL, false)

	writeValueConcurrency = runtime.NumCPU()
)

func createHandler(hndlr Handler, versionCheck bool) Handler {
	return func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
		w.Header().Set(NomsVersionHeader, constants.NomsVersion)

		if versionCheck && req.Header.Get(NomsVersionHeader) != constants.NomsVersion {
			verbose.Log("Returning version mismatch error")
			http.Error(
				w,
				fmt.Sprintf("Error: SDK version %s is incompatible with data of version %s", req.Header.Get(NomsVersionHeader), constants.NomsVersion),
				http.StatusBadRequest,
			)
			return
		}

		err := d.Try(func() { hndlr(w, req, ps, cs) })
		if err != nil {
			err = d.Unwrap(err)
			verbose.Log("Returning bad request:\n%v\n", err)
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
			return
		}
	}
}

func handleWriteValue(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "POST" {
		d.Panic("Expected post method.")
	}

	t1 := time.Now()
	totalDataWritten := 0
	chunkCount := 0

	verbose.Log("Handling WriteValue from " + req.RemoteAddr)
	defer func() {
		verbose.Log("Wrote %d Kb as %d chunks from %s in %s", totalDataWritten/1024, chunkCount, req.RemoteAddr, time.Since(t1))
	}()

	reader := bodyReader(req)
	defer func() {
		// Ensure all data on reader is consumed
		io.Copy(ioutil.Discard, reader)
		reader.Close()
	}()
	vbs := types.NewValidatingBatchingSink(cs)
	vbs.Prepare(deserializeHints(reader))

	// Deserialize chunks from reader in background, recovering from errors
	errChan := make(chan error)
	chunkChan := make(chan *chunks.Chunk, writeValueConcurrency)

	go func() {
		var err error
		defer func() { errChan <- err; close(errChan) }()
		defer close(chunkChan)
		err = chunks.Deserialize(reader, chunkChan)
	}()

	decoded := make(chan chan types.DecodedChunk, writeValueConcurrency)

	go func() {
		defer close(decoded)
		for c := range chunkChan {
			ch := make(chan types.DecodedChunk)
			decoded <- ch

			go func(ch chan types.DecodedChunk, c *chunks.Chunk) {
				ch <- vbs.DecodeUnqueued(c)
			}(ch, c)
		}
	}()

	var bpe chunks.BackpressureError
	for ch := range decoded {
		dc := <-ch
		if dc.Chunk != nil && dc.Value != nil {
			if bpe == nil {
				totalDataWritten += len(dc.Chunk.Data())
				bpe = vbs.Enqueue(*dc.Chunk, *dc.Value)
				chunkCount++
				if chunkCount%100 == 0 {
					verbose.Log("Enqueued %d chunks", chunkCount)
				}
			} else {
				bpe = append(bpe, dc.Chunk.Hash())
			}
			// If a previous Enqueue() errored, we still need to drain chunkChan
			// TODO: what about having Deserialize take a 'done' channel to stop it?
		}
	}

	// If there was an error during chunk deserialization, raise so it can be logged and responded to.
	if err := <-errChan; err != nil {
		panic(d.Wrap(fmt.Errorf("Deserialization failure: %v", err)))
	}

	if bpe == nil {
		bpe = vbs.Flush()
	}
	if bpe != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Add("Content-Type", "application/octet-stream")
		writer := respWriter(req, w)
		defer writer.Close()
		serializeHashes(writer, bpe.AsHashes())
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// Contents of the returned io.Reader are snappy-compressed.
func buildWriteValueRequest(chunkChan chan *chunks.Chunk, hints map[hash.Hash]struct{}) io.Reader {
	body, pw := io.Pipe()

	go func() {
		gw := snappy.NewBufferedWriter(pw)
		serializeHints(gw, hints)
		for c := range chunkChan {
			chunks.Serialize(*c, gw)
		}
		d.Chk.NoError(gw.Close())
		d.Chk.NoError(pw.Close())
	}()

	return body
}

func bodyReader(req *http.Request) (reader io.ReadCloser) {
	reader = req.Body
	if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.PanicIfError(err)
		reader = gr
	} else if strings.Contains(req.Header.Get("Content-Encoding"), "x-snappy-framed") {
		sr := snappy.NewReader(reader)
		reader = ioutil.NopCloser(sr)
	}
	return
}

func respWriter(req *http.Request, w http.ResponseWriter) (writer io.WriteCloser) {
	writer = wc{w.(io.Writer)}
	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Add("Content-Encoding", "gzip")
		gw := gzip.NewWriter(w)
		writer = gw
	} else if strings.Contains(req.Header.Get("Accept-Encoding"), "x-snappy-framed") {
		w.Header().Add("Content-Encoding", "x-snappy-framed")
		sw := snappy.NewBufferedWriter(w)
		writer = sw
	}
	return
}

type wc struct {
	io.Writer
}

func (wc wc) Close() error {
	return nil
}

func handleGetRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "POST" {
		d.Panic("Expected post method.")
	}

	hashes := extractHashes(req)

	w.Header().Add("Content-Type", "application/octet-stream")
	writer := respWriter(req, w)
	defer writer.Close()

	for len(hashes) > 0 {
		batch := hashes

		// Limit RAM consumption by streaming chunks in ~8MB batches
		if len(batch) > maxGetBatchSize {
			batch = batch[:maxGetBatchSize]
		}

		chunkChan := make(chan *chunks.Chunk, maxGetBatchSize)
		go func() {
			cs.GetMany(hashes.HashSet(), chunkChan)
			close(chunkChan)
		}()

		for c := range chunkChan {
			chunks.Serialize(*c, writer)
		}

		hashes = hashes[len(batch):]
	}
}

func handleGetBlob(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	refStr := req.URL.Query().Get("h")
	if refStr == "" {
		d.Panic("Expected h param")
	}

	h := hash.Parse(refStr)
	if (h == hash.Hash{}) {
		d.Panic("h failed to parse")
	}

	vs := types.NewValueStore(types.NewBatchStoreAdaptor(cs))
	v := vs.ReadValue(h)
	b, ok := v.(types.Blob)
	if !ok {
		d.Panic("h is not a Blob")
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", b.Len()))
	w.Header().Add("Cache-Control", fmt.Sprintf("max-age=%d", 60*60*24*365))

	b.Reader().Copy(w)
}

func extractHashes(req *http.Request) hash.HashSlice {
	err := req.ParseForm()
	d.PanicIfError(err)
	hashStrs := req.PostForm["ref"]
	if len(hashStrs) <= 0 {
		d.Panic("PostForm is empty")
	}

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

func handleHasRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "POST" {
		d.Panic("Expected post method.")
	}

	hashes := extractHashes(req)

	w.Header().Add("Content-Type", "text/plain")
	writer := respWriter(req, w)
	defer writer.Close()

	for _, h := range hashes {
		fmt.Fprintf(writer, "%s %t\n", h, cs.Has(h))
	}
}

func handleRootGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	if req.Method != "GET" {
		d.Panic("Expected get method.")
	}

	rootRef := rt.Root()
	fmt.Fprintf(w, "%v", rootRef.String())
	w.Header().Add("content-type", "text/plain")
}

func handleRootPost(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "POST" {
		d.Panic("Expected post method.")
	}

	params := req.URL.Query()
	tokens := params["last"]
	if len(tokens) != 1 {
		d.Panic(`Expected "last" query param value`)
	}
	last := hash.Parse(tokens[0])
	tokens = params["current"]
	if len(tokens) != 1 {
		d.Panic(`Expected "current" query param value`)
	}
	current := hash.Parse(tokens[0])

	vs := types.NewValueStore(types.NewBatchStoreAdaptor(cs))

	// Ensure that proposed new Root is present in cs
	proposed := vs.ReadValue(current)
	if proposed == nil {
		d.Panic("Can't set Root to a non-present Chunk")
	}

	// Even though the Root is actually a Map<String, Ref<Commit>>, its Noms Type is Map<String, Ref<Value>> in order to prevent the root chunk from getting bloated with type info. That means that the Value of the proposed new Root needs to be manually type-checked. The simplest way to do that would be to iterate over the whole thing and pull the target of each Ref from |cs|. That's a lot of reads, though, and it's more efficient to just read the Value indicated by |last|, diff the proposed new root against it, and validate whatever new entries appear.
	datasets := types.NewMap()
	if !last.IsEmpty() {
		lastVal := vs.ReadValue(last)
		if lastVal == nil {
			d.Panic("Can't UpdateRoot from a non-present Chunk")
		}

		datasets = lastVal.(types.Map)
	}

	// Ensure that proposed new Root is a Map and, if it has anything in it, that it's <String, <Ref<Commit>>

	if m, ok := proposed.(types.Map); !ok {
		d.Panic("Root of a Database must be a Map")
	} else if !m.Empty() {
		assertMapOfStringToRefOfCommit(m, datasets, vs)
	}

	if !cs.UpdateRoot(current, last) {
		w.WriteHeader(http.StatusConflict)
		return
	}
}

func handleGraphQL(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != http.MethodGet && req.Method != http.MethodPost {
		d.Panic("Unexpected method")
	}

	ds := req.FormValue("ds")
	h := req.FormValue("h")

	if (ds == "") == (h == "") {
		d.Panic("Must specify one (and only one) of ds (dataset) or h (hash)")
	}

	query := req.FormValue("query")
	if query == "" {
		d.Panic("Expected query")
	}

	// Note: we don't close this becaues |cs| will be closed by the generic endpoint handler
	db := NewDatabase(cs)

	var rootValue types.Value
	var err error
	if ds != "" {
		dataset := db.GetDataset(ds)
		var ok bool
		rootValue, ok = dataset.MaybeHead()
		if !ok {
			err = fmt.Errorf("Dataset %s not found", ds)
		}
	} else {
		rootValue = db.ReadValue(hash.Parse(h))
		if rootValue == nil {
			err = errors.New("Root value not found")
		}
	}

	w.Header().Add("Content-Type", "application/json")
	writer := respWriter(req, w)
	defer writer.Close()

	if err != nil {
		ngql.Error(err, writer)
	} else {
		ngql.Query(rootValue, query, db, writer)
	}
}

func handleBaseGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	if req.Method != "GET" {
		d.Panic("Expected get method.")
	}

	w.Header().Add("Content-Type", "text/html")
	fmt.Fprintf(w, nomsBaseHTML)
}

func assertMapOfStringToRefOfCommit(proposed, datasets types.Map, vr types.ValueReader) {
	stopChan := make(chan struct{})
	defer close(stopChan)
	changes := make(chan types.ValueChanged)
	go func() {
		defer close(changes)
		proposed.Diff(datasets, changes, stopChan)
	}()
	for change := range changes {
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			// Since this is a Map Diff, change.V is the key at which a change was detected.
			// Go get the Value there, which should be a Ref<Value>, deref it, and then ensure the target is a Commit.
			val := proposed.Get(change.V)
			ref, ok := val.(types.Ref)
			if !ok {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, but key %s maps to a %s", change.V.(types.String), val.Type().Describe())
			}
			if targetType := ref.TargetValue(vr).Type(); !IsCommitType(targetType) {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, not the ref at key %s points to a %s", change.V.(types.String), targetType.Describe())
			}
		}
	}
}
