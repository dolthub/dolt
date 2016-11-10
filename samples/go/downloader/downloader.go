// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var (
	lastSourcePathFieldName = "sourcePath"
	foundInCacheCnt         = &counter{}
)

func main() {
	if !download() {
		exit.Fail()
	}
}

func usage() {
	fmt.Println("usage: downloader [--cache-ds <dsname>] [--concurrency <int>] <in-path> <outdsname>")
	flag.PrintDefaults()
}

type RemoteResource struct {
	Url string
}

type LocalResource struct {
	Url        string
	Downloaded bool
	Err        string    `noms:",omitempty"`
	BlobRef    types.Ref `noms:",omitempty"`
}

func download() (win bool) {
	var cacheDsArg = flag.String("cache-ds", "", "name of photo-cache dataset")
	var concurrencyArg = flag.Uint("concurrency", 4, "number of concurrent HTTP calls to retrieve remote resources")
	verbose.RegisterVerboseFlags(flag.CommandLine)
	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "error: missing required argument")
		flag.Usage()
		return
	}
	inPath := flag.Arg(0)
	outDsName := flag.Arg(1)

	if *concurrencyArg < 1 {
		fmt.Fprintln(os.Stderr, "error, concurrency cannot be less than 1")
		flag.Usage()
		return
	}

	// Resolve the in-path arg and get the inRoot
	cfg := config.NewResolver()
	db, inRoot, err := cfg.GetPath(inPath)
	if err != nil || inRoot == nil {
		if err == nil {
			err = errors.New("Could not find referenced value.")
		}
		fmt.Fprintf(os.Stderr, "Invalid input path '%s': %s\n", inPath, err)
		return
	}

	if datas.IsCommitType(inRoot.Type()) {
		fmt.Fprintln(os.Stderr, "Input cannot be a commit. Consider appending '.value' to your in-path argument")
		return
	}

	// In order to pin the path, we need to get the path after running it through
	// the config file processing.
	resolvedPath := cfg.ResolvePathSpec(inPath)
	inSpec, err := spec.ForPath(resolvedPath)
	d.PanicIfError(err)
	defer inSpec.Close()

	pinnedPath := pinPath(db, inSpec.Path)
	fmt.Println("Resolved in-path:", resolvedPath, "\nPinned path:", pinnedPath)

	// Get the current head of out-ds. If there is one, assume it was created
	// by an earlier run of this program.
	var lastOutCommit types.Value
	if loc, ok := db.GetDataset(outDsName).MaybeHead(); ok {
		lastOutCommit = loc
		if lastOutCommit != nil {
			fmt.Println("Last out commit:", lastOutCommit.Hash())
		}
	}

	// If there was an earlier version of out-ds, then look in the meta info
	// on the commit to see if there is a record of the inRoot used by the
	// previous run. If so, we can do an incremental sync.
	var lastInRoot types.Value
	if lastOutCommit != nil {
		lastInRoot = getLastInRoot(db, lastOutCommit.(types.Struct))
		if lastInRoot != nil {
			fmt.Println("lastInRoot:", lastInRoot.Hash())
		}
	}

	// Get a resourceCache specified by the cache-ds arg
	var cache *resourceCache
	if *cacheDsArg != "" {
		cache, err = getResourceCache(db, *cacheDsArg)
		if err != nil {
			fmt.Println("error: ", err)
			flag.Usage()
			return
		}
	}

	newRoot := downloadPhotos(db, inRoot, lastInRoot, lastOutCommit, cache, *cacheDsArg, *concurrencyArg)

	// Commit latest value for resourceCache
	if cache != nil {
		d.PanicIfError(cache.commit(db, *cacheDsArg))
	}

	// Commit new root
	meta := newMeta(db, pinnedPath.String())
	outDs := db.GetDataset(outDsName)
	if _, err = db.Commit(outDs, newRoot, datas.CommitOptions{Meta: meta}); err != nil {
		fmt.Fprintf(os.Stderr, "Could not commit: %s\n", err)
		return
	}

	win = true
	return
}

func downloadPhotos(db datas.Database, inRoot, lastInRoot, lastOutCommit types.Value, cache *resourceCache, cacheDsName string, concurrency uint) (newRoot types.Value) {
	// return true whenever we find a RemoteResource
	shouldCnt := &counter{}
	shouldUpdateCb := func(p types.Path, root, parent, v types.Value) (res bool) {
		shouldCnt.Increment()
		return v != nil && v.Type().Kind() == types.StructKind && v.Type().Desc.(types.StructDesc).Name == "RemoteResource"
	}

	updateCnt := &counter{}
	failedCnt := &counter{}
	rwMutex := sync.RWMutex{}

	// Use info from dif to create a new RemoteResource and return it.
	// Also keeps a counter of how many times this has been called and commits
	// the current state of the cache.
	// Todo: remove rwMutex when issue #2792 is resolved
	updateCb := func(dif diff.Difference) diff.Difference {
		doDownload := func(db datas.Database, url string, cache *resourceCache) LocalResource {
			rwMutex.RLock()
			defer rwMutex.RUnlock()
			return downloadRemoteResource(db, url, cache)
		}

		var remote RemoteResource
		err := marshal.Unmarshal(dif.OldValue, &remote)
		d.PanicIfError(err)

		localResource := doDownload(db, remote.Url, cache)
		if !localResource.Downloaded {
			failedCnt.Increment()
		}
		newValue, err := marshal.Marshal(localResource)
		d.PanicIfError(err)
		dif.NewValue = newValue

		updateCnt.Increment()
		if cache != nil && updateCnt.Cnt()%1000 == 0 {
			rwMutex.Lock()
			defer rwMutex.Unlock()
			err := cache.commit(db, cacheDsName)
			d.PanicIfError(err)
		}

		status.Printf("walked: %d, updated %d, found in cache: %d, errors retrieving: %d", shouldCnt.Cnt(), updateCnt.Cnt(), foundInCacheCnt.Cnt(), failedCnt.Cnt())
		return dif
	}

	if lastInRoot != nil && lastInRoot.Equals(inRoot) {
		// The current inRoot is the same as the last one we worked on, so there
		// is nothing to do. Just return the inRoot so a new commit can be added
		// latest meta data
		fmt.Println("No change since last run, doing nothing")
		return inRoot
	}

	var lastOutRoot types.Value
	if lastOutCommit != nil {
		lastOutRoot = lastOutCommit.(types.Struct).Get("value")
	}

	newRoot = IncrementalUpdate(db, inRoot, lastInRoot, lastOutRoot, shouldUpdateCb, updateCb, concurrency)
	status.Done()
	return
}

// downloadRemoteResource takes a url and creates a LocalResource by making an
// HTTP call to get the resource and storing it locally.
func downloadRemoteResource(db datas.Database, url string, cache *resourceCache) LocalResource {
	errorstring := ""
	downloaded := true
	blobRef, err := downloadAndCacheBlob(db, url, cache)
	if err != nil {
		errorstring = err.Error()
		downloaded = false
	}
	return LocalResource{Url: url, Downloaded: downloaded, Err: errorstring, BlobRef: blobRef}
}

// downloadAndCacheBlob wraps downloadBlob in a wrapper that first checks the
// cache to see if the blob has already been stored and then adding the blob to
// a persistent cache once it has been retrieved.
func downloadAndCacheBlob(db datas.Database, url string, cache *resourceCache) (types.Ref, error) {
	if cache == nil {
		return downloadBlob(db, url)
	}

	nurl := types.String(url)
	hs := types.String(nurl.Hash().String())
	if blobRef, ok := cache.get(hs); ok {
		foundInCacheCnt.Increment()
		return blobRef, nil
	}
	blobRef, err := downloadBlob(db, url)
	if err != nil {
		return types.Ref{}, err
	}
	cache.set(hs, blobRef)
	return blobRef, nil
}

// downloadBlob makes the http call to get the resource and store it in a blob
func downloadBlob(db datas.Database, url string) (types.Ref, error) {
	resp, err := http.Get(url)
	if err != nil {
		return types.Ref{}, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("bad status from http download request, status code: %d, status: %s", resp.StatusCode, resp.Status)
		return types.Ref{}, err
	}

	defer resp.Body.Close()

	blob := types.NewStreamingBlob(db, resp.Body)
	return types.NewRef(blob), nil
}

// getLastInRoot checks the lastOutPhotos struct to see if it contains a "meta"
// attribute struct that has an InPath field. If so, and if it's able to resolve
// the path, it's returned to be used for incremental update.
func getLastInRoot(db datas.Database, lastOutPhotos types.Struct) (res types.Value) {
	var metaV types.Value
	var ok bool
	if metaV, ok = lastOutPhotos.MaybeGet("meta"); !ok {
		return
	}

	meta := metaV.(types.Struct)
	var lastInRootSpecV types.Value
	if lastInRootSpecV, ok = meta.MaybeGet(lastSourcePathFieldName); !ok {
		return
	}

	lastInRootSpec := string(lastInRootSpecV.(types.String))
	absPath, err := spec.NewAbsolutePath(lastInRootSpec)
	if err != nil {
		return
	}
	res = absPath.Resolve(db)
	return
}

func newMeta(db datas.Database, source string) types.Struct {
	meta, err := spec.CreateCommitMetaStruct(db, "", "", map[string]string{lastSourcePathFieldName: source}, nil)
	d.PanicIfError(err)
	return meta
}

// pinPath takes an absolute path. If it begins with a dataset, it changes it
// to begin with a hash of the current dataset head.
func pinPath(db datas.Database, absPath spec.AbsolutePath) spec.AbsolutePath {
	h := absPath.Hash
	if h.IsEmpty() {
		r, ok := db.GetDataset(absPath.Dataset).MaybeHeadRef()
		d.PanicIfFalse(ok)
		h = r.TargetHash()
	}
	return spec.AbsolutePath{Hash: h, Path: absPath.Path}
}

type counter struct {
	cnt   uint32
	mutex sync.Mutex
}

func (uc *counter) Cnt() uint32 {
	uc.mutex.Lock()
	defer uc.mutex.Unlock()
	return uc.cnt
}
func (uc *counter) Increment() {
	uc.mutex.Lock()
	defer uc.mutex.Unlock()
	uc.cnt += 1
}
