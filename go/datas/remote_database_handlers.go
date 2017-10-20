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
	"log"
	"net/http"
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
	maxGetBatchSize   = 1 << 14 // Limit GetMany() to ~16k chunks, or ~64MB of data
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

	HandleStats = createHandler(handleStats, false)

	writeValueConcurrency = runtime.NumCPU()
)

func createHandler(hndlr Handler, versionCheck bool) Handler {
	return func(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
		w.Header().Set(NomsVersionHeader, constants.NomsVersion)

		if versionCheck && req.Header.Get(NomsVersionHeader) != constants.NomsVersion {
			log.Printf("returning version mismatch error")
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
			log.Printf("returning bad request error: %v", err)
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
	vdc := types.NewValidatingDecoder(cs)

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
				ch <- vdc.Decode(c)
			}(ch, c)
		}
	}()

	unresolvedRefs := hash.HashSet{}
	for ch := range decoded {
		dc := <-ch
		if dc.Chunk != nil && dc.Value != nil {
			(*dc.Value).WalkRefs(func(r types.Ref) {
				unresolvedRefs.Insert(r.TargetHash())
			})

			totalDataWritten += len(dc.Chunk.Data())
			cs.Put(*dc.Chunk)
			chunkCount++
			if chunkCount%100 == 0 {
				verbose.Log("Enqueued %d chunks", chunkCount)
			}
		}
	}

	// If there was an error during chunk deserialization, raise so it can be logged and responded to.
	if err := <-errChan; err != nil {
		d.Panic("Deserialization failure: %v", err)
	}

	if chunkCount > 0 {
		types.PanicIfDangling(unresolvedRefs, cs)
		persistChunks(cs)
	}

	w.WriteHeader(http.StatusCreated)
}

// Contents of the returned io.ReadCloser are snappy-compressed.
func buildWriteValueRequest(chunkChan chan *chunks.Chunk) io.ReadCloser {
	body, pw := io.Pipe()

	go func() {
		sw := snappy.NewBufferedWriter(pw)
		defer checkClose(pw)
		defer checkClose(sw)
		for c := range chunkChan {
			chunks.Serialize(*c, sw)
		}
	}()

	return body
}

func checkClose(c io.Closer) {
	d.PanicIfError(c.Close())
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

func persistChunks(cs chunks.ChunkStore) {
	for !cs.Commit(cs.Root(), cs.Root()) {
	}
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
			cs.GetMany(batch.HashSet(), chunkChan)
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

	vs := types.NewValueStore(cs)
	v := vs.ReadValue(h)
	b, ok := v.(types.Blob)
	if !ok {
		d.Panic("h is not a Blob")
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", b.Len()))
	w.Header().Add("Cache-Control", fmt.Sprintf("max-age=%d", 60*60*24*365))

	b.Copy(w)
}

func extractHashes(req *http.Request) hash.HashSlice {
	reader := bodyReader(req)
	defer reader.Close()
	defer io.Copy(ioutil.Discard, reader) // Ensure all data on reader is consumed
	return deserializeHashes(reader)
}

func BuildHashesRequestForTest(hashes hash.HashSet) io.ReadCloser {
	batch := chunks.ReadBatch{}
	for h := range hashes {
		batch[h] = nil
	}
	return buildHashesRequest(batch)
}

func buildHashesRequest(batch chunks.ReadBatch) io.ReadCloser {
	body, pw := io.Pipe()
	go func() {
		defer checkClose(pw)
		serializeHashes(pw, batch)
	}()
	return body
}

func handleHasRefs(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "POST" {
		d.Panic("Expected post method.")
	}

	hashes := extractHashes(req)

	w.Header().Add("Content-Type", "text/plain")
	writer := respWriter(req, w)
	defer writer.Close()

	absent := cs.HasMany(hashes.HashSet())
	for h := range absent {
		fmt.Fprintln(writer, h.String())
	}
}

func handleRootGet(w http.ResponseWriter, req *http.Request, ps URLParams, rt chunks.ChunkStore) {
	if req.Method != "GET" {
		d.Panic("Expected get method.")
	}
	fmt.Fprintf(w, "%v", rt.Root().String())
	w.Header().Add("content-type", "text/plain")
}

func handleStats(w http.ResponseWriter, req *http.Request, ps URLParams, cs chunks.ChunkStore) {
	if req.Method != "GET" {
		d.Panic("Expected get method.")
	}
	fmt.Fprint(w, cs.StatsSummary())
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
	// "current" should really, really be called "proposed" or something in the wire API
	tokens = params["current"]
	if len(tokens) != 1 {
		d.Panic(`Expected "current" query param value`)
	}
	proposed := hash.Parse(tokens[0])

	vs := types.NewValueStore(cs)

	// Even though the Root is actually a Map<String, Ref<Commit>>, its Noms Type is Map<String, Ref<Value>> in order to prevent the root chunk from getting bloated with type info. That means that the Value of the proposed new Root needs to be manually type-checked. The simplest way to do that would be to iterate over the whole thing and pull the target of each Ref from |cs|. That's a lot of reads, though, and it's more efficient to just read the Value indicated by |last|, diff the proposed new root against it, and validate whatever new entries appear.
	lastMap := validateLast(last, vs)

	proposedMap := validateProposed(proposed, last, vs)
	if !proposedMap.Empty() {
		assertMapOfStringToRefOfCommit(proposedMap, lastMap, vs)
	}

	// If some other client has committed to |vs| since it had |from| at the
	// root, this call to vs.Commit() will fail. Used to be that we'd always
	// propagate that failure back to the client and let them try again. This
	// made one very common operation annoyingly expensive, though, as clients
	// simultaneously committing to different Datasets would cause conflicts
	// with this vs.Commit() right here. In this common case, the server
	// already knows everything it needs to try again, so now we cut out the
	// round trip to the client and just retry inline.
	for to, from := proposed, last; !vs.Commit(to, from); {
		// If committing failed, we go read out the map of Datasets at the root of the store, which is a Map[string]Ref<Commit>
		rootMap := types.NewMap(vs)
		root := vs.Root()
		if v := vs.ReadValue(root); v != nil {
			rootMap = v.(types.Map)
		}

		// Since we know that lastMap is an ancestor of both proposedMap and
		// rootMap, we can try to do a three-way merge here. We don't want to
		// traverse the Ref<Commit>s stored in the maps, though, just
		// basically merge the maps together as long the changes to rootMap
		// and proposedMap were in different Datasets.
		merged, err := mergeDatasetMaps(proposedMap, rootMap, lastMap, vs)
		if err != nil {
			verbose.Log("Attempted root map auto-merge failed: %s", err)
			w.WriteHeader(http.StatusConflict)
			break
		}
		to, from = vs.WriteValue(merged).TargetHash(), root
	}

	// If committing succeeded, the root of the store might be |proposed|...or
	// it might be some result of the merge performed above. So, we need to
	// tell the client what the new root is. If the commit failed, obviously
	// we need to inform the client of the actual current root.
	w.Header().Add("content-type", "text/plain")
	fmt.Fprintf(w, "%v", vs.Root().String())
}

func validateLast(last hash.Hash, vrw types.ValueReadWriter) types.Map {
	if last.IsEmpty() {
		return types.NewMap(vrw)
	}
	lastVal := vrw.ReadValue(last)
	if lastVal == nil {
		d.Panic("Can't Commit from a non-present Chunk")
	}
	return lastVal.(types.Map)
}

func validateProposed(proposed, last hash.Hash, vrw types.ValueReadWriter) types.Map {
	// Only allowed to skip this check if both last and proposed are empty, because that represents the special case of someone flushing chunks into an empty store.
	if last.IsEmpty() && proposed.IsEmpty() {
		return types.NewMap(vrw)
	}
	// Ensure that proposed new Root is present in vr, is a Map and, if it has anything in it, that it's <String, <Ref<Commit>>
	proposedVal := vrw.ReadValue(proposed)
	if proposedVal == nil {
		d.Panic("Can't set Root to a non-present Chunk")
	}

	proposedMap, ok := proposedVal.(types.Map)
	if !ok {
		d.Panic("Root of a Database must be a Map")
	}
	return proposedMap
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
			val := change.NewValue
			ref, ok := val.(types.Ref)
			if !ok {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, but key %s maps to a %s", change.Key.(types.String), types.TypeOf(val).Describe())
			}
			if targetValue := ref.TargetValue(vr); !IsCommit(targetValue) {
				d.Panic("Root of a Database must be a Map<String, Ref<Commit>>, but the ref at key %s points to a %s", change.Key.(types.String), types.TypeOf(targetValue).Describe())
			}
		}
	}
}

func mergeDatasetMaps(a, b, parent types.Map, vrw types.ValueReadWriter) (types.Map, error) {
	aChangeChan, bChangeChan := make(chan types.ValueChanged), make(chan types.ValueChanged)
	stopChan := make(chan struct{})

	go func() {
		defer close(aChangeChan)
		a.Diff(parent, aChangeChan, stopChan)
	}()
	go func() {
		defer close(bChangeChan)
		b.Diff(parent, bChangeChan, stopChan)
	}()
	defer func() {
		close(stopChan)
		for range aChangeChan {
		}
		for range bChangeChan {
		}
	}()

	apply := func(target *types.MapEditor, change types.ValueChanged, newVal types.Value) *types.MapEditor {
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			return target.Set(change.Key, newVal)
		case types.DiffChangeRemoved:
			return target.Remove(change.Key)
		default:
			panic("Not Reached")
		}
	}

	merged := parent.Edit()
	aChange, bChange := types.ValueChanged{}, types.ValueChanged{}
	for {
		if aChange.Key == nil {
			aChange = <-aChangeChan
		}
		if bChange.Key == nil {
			bChange = <-bChangeChan
		}

		// Both channels are producing zero values, so we're done.
		if aChange.Key == nil && bChange.Key == nil {
			break
		}

		if aChange.Key != nil && (bChange.Key == nil || aChange.Key.Less(bChange.Key)) {
			merged = apply(merged, aChange, a.Get(aChange.Key))
			aChange = types.ValueChanged{}
			continue
		} else if bChange.Key != nil && (aChange.Key == nil || bChange.Key.Less(aChange.Key)) {
			merged = apply(merged, bChange, b.Get(bChange.Key))
			bChange = types.ValueChanged{}
			continue
		}

		d.PanicIfFalse(aChange.Key.Equals(bChange.Key))
		// If the two diffs generate different kinds of changes at the same key, conflict.
		if aChange.ChangeType != bChange.ChangeType {
			return parent, errors.New("Incompatible changes at " + types.EncodedValue(aChange.Key))
		}

		// Otherwise, we're OK IFF the two diffs made exactly the same change
		aValue := a.Get(aChange.Key)
		if aChange.ChangeType != types.DiffChangeRemoved && !aValue.Equals(b.Get(bChange.Key)) {
			return parent, errors.New("Incompatible changes at " + types.EncodedValue(aChange.Key))
		}
		merged = apply(merged, aChange, aValue)
		aChange, bChange = types.ValueChanged{}, types.ValueChanged{}
	}
	return merged.Map(), nil
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
