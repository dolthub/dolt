// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/golang/snappy"
)

type URLParams interface {
	ByName(string) string
}

type Handler func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore)

// NomsVersionHeader is the name of the header that Noms clients and servers must set in every request/response.
const NomsVersionHeader = "x-noms-vers"
const nomsBaseHtml = "<html><head></head><body><p>Hi. This is a Noms HTTP server.</p><p>To learn more, visit <a href=\"https://github.com/attic-labs/noms\">our GitHub project</a>.</p></body></html>"

var (
	// HandleWriteValue is meant to handle HTTP POST requests to the writeValue/ server endpoint. The payload should be an appropriately-ordered sequence of Chunks to be validated and stored on the server.
	// TODO: Nice comment about what headers it expects/honors, payload format, and error responses.
	HandleWriteValue = versionCheck(handleWriteValue)

	// HandleGetRefs is meant to handle HTTP POST requests to the getRefs/ server endpoint. Given a sequence of Chunk hashes, the server will fetch and return them.
	// TODO: Nice comment about what headers it expects/honors, payload format, and responses.
	HandleGetRefs = versionCheck(handleGetRefs)

	// HandleWriteValue is meant to handle HTTP POST requests to the hasRefs/ server endpoint. Given a sequence of Chunk hashes, the server check for their presence and return a list of true/false responses.
	// TODO: Nice comment about what headers it expects/honors, payload format, and responses.
	HandleHasRefs = versionCheck(handleHasRefs)

	// HandleRootGet is meant to handle HTTP GET requests to the root/ server endpoint. The server returns the hash of the Root as a string.
	// TODO: Nice comment about what headers it expects/honors, payload format, and responses.
	HandleRootGet = versionCheck(handleRootGet)

	// HandleWriteValue is meant to handle HTTP POST requests to the root/ server endpoint. This is used to update the Root to point to a new Chunk.
	// TODO: Nice comment about what headers it expects/honors, payload format, and error responses.
	HandleRootPost = versionCheck(handleRootPost)

	// HandleBaseGet is meant to handle HTTP GET requests to the / server endpoint. This is used to give a friendly message to users.
	// TODO: Nice comment about what headers it expects/honors, payload format, and error responses.
	HandleBaseGet = handleBaseGet
)

func versionCheck(hndlr Handler) Handler {
	return func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
		w.Header().Set(NomsVersionHeader, constants.NomsVersion)
		if req.Header.Get(NomsVersionHeader) != constants.NomsVersion {
			fmt.Println("Returning version mismatch error")
			http.Error(
				w,
				fmt.Sprintf("Error: SDK version %s is incompatible with data of version %s", req.Header.Get(NomsVersionHeader), constants.NomsVersion),
				http.StatusBadRequest,
			)
			return
		}

		err := d.Try(func() { hndlr(w, req, ps, cs) })
		if err != nil {
			fmt.Printf("Returning bad request: %v\n", err)
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
			return
		}
	}
}

func handleWriteValue(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	d.PanicIfTrue(req.Method != "POST", "Expected post method.")

	reader := bodyReader(req)
	defer func() {
		// Ensure all data on reader is consumed
		io.Copy(ioutil.Discard, reader)
		reader.Close()
	}()
	tc := types.NewTypeCache()
	vbs := types.NewValidatingBatchingSink(cs, tc)
	vbs.Prepare(deserializeHints(reader))

	chunkChan := make(chan *chunks.Chunk, 16)
	go chunks.DeserializeToChan(reader, chunkChan)
	var bpe chunks.BackpressureError
	for c := range chunkChan {
		if bpe == nil {
			bpe = vbs.Enqueue(*c)
		} else {
			bpe = append(bpe, c.Hash())
		}
		// If a previous Enqueue() errored, we still need to drain chunkChan
		// TODO: what about having DeserializeToChan take a 'done' channel to stop it?
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
	d.PanicIfTrue(req.Method != "POST", "Expected post method.")

	hashes := extractHashes(req)

	w.Header().Add("Content-Type", "application/octet-stream")
	writer := respWriter(req, w)
	defer writer.Close()

	for _, h := range hashes {
		c := cs.Get(h)
		if !c.IsEmpty() {
			chunks.Serialize(c, writer)
		}
	}
}

func extractHashes(req *http.Request) hash.HashSlice {
	err := req.ParseForm()
	d.PanicIfError(err)
	hashStrs := req.PostForm["ref"]
	d.PanicIfTrue(len(hashStrs) <= 0, "PostForm is empty")

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
	d.PanicIfTrue(req.Method != "POST", "Expected post method.")

	hashes := extractHashes(req)

	w.Header().Add("Content-Type", "text/plain")
	writer := respWriter(req, w)
	defer writer.Close()

	for _, h := range hashes {
		fmt.Fprintf(writer, "%s %t\n", h, cs.Has(h))
	}
}

func handleRootGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	d.PanicIfTrue(req.Method != "GET", "Expected get method.")

	rootRef := rt.Root()
	fmt.Fprintf(w, "%v", rootRef.String())
	w.Header().Add("content-type", "text/plain")
}

func handleRootPost(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	d.PanicIfTrue(req.Method != "POST", "Expected post method.")

	params := req.URL.Query()
	tokens := params["last"]
	d.PanicIfTrue(len(tokens) != 1, `Expected "last" query param value`)
	last := hash.Parse(tokens[0])
	tokens = params["current"]
	d.PanicIfTrue(len(tokens) != 1, `Expected "current" query param value`)
	current := hash.Parse(tokens[0])

	// Ensure that proposed new Root is present in cs
	c := cs.Get(current)
	d.PanicIfTrue(c.IsEmpty(), "Can't set Root to a non-present Chunk")

	// Ensure that proposed new Root is a Map and, if it has anything in it, that it's <String, <Ref<Commit>>
	v := types.DecodeValue(c, nil)
	d.PanicIfTrue(v.Type().Kind() != types.MapKind, "Root of a Database must be a Map")
	m := v.(types.Map)
	if !m.Empty() && !isMapOfStringToRefOfCommit(m) {
		panic(d.Wrap(fmt.Errorf("Root of a Database must be a Map<String, Ref<Commit>>, not %s", m.Type().Describe())))
	}

	if !cs.UpdateRoot(current, last) {
		w.WriteHeader(http.StatusConflict)
		return
	}
}

func handleBaseGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	d.PanicIfTrue(req.Method != "GET", "Expected get method.")

	w.Header().Add("content-type", "text/html")
	fmt.Fprintf(w, nomsBaseHtml)
}

func isMapOfStringToRefOfCommit(m types.Map) bool {
	mapTypes := m.Type().Desc.(types.CompoundDesc).ElemTypes
	keyType, valType := mapTypes[0], mapTypes[1]
	return keyType.Kind() == types.StringKind && (IsRefOfCommitType(valType) || isUnionOfRefOfCommitType(valType))
}

func isUnionOfRefOfCommitType(t *types.Type) bool {
	if t.Kind() != types.UnionKind {
		return false
	}
	for _, et := range t.Desc.(types.CompoundDesc).ElemTypes {
		if !IsRefOfCommitType(et) {
			return false
		}
	}
	return true
}
