// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	flag "github.com/juju/gnuflag"
	"github.com/julienschmidt/httprouter"
)

const (
	dsPathPrefix = "/-ds"
)

var (
	portFlag = flag.Int("port", 0, "Port to listen on")
)

type chunkStoreRecord struct {
	cs    chunks.ChunkStore
	alias string
}

type chunkStoreRecords map[string]chunkStoreRecord

func main() {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: noms ui [-host HOST] directory [args...]\n")
		fmt.Fprintln(os.Stderr, "  args are of the form arg1=val1, arg2=val2, etc. \"ldb:\" values are automatically translated into paths to an HTTP noms database server.\n")
		flag.PrintDefaults()
	}

	flag.Parse(true)
	flag.Usage = usage

	if len(flag.Args()) == 0 {
		usage()
		os.Exit(1)
	}

	uiDir := flag.Arg(0)
	qsValues, stores := constructQueryString(flag.Args()[1:])

	router := &httprouter.Router{
		HandleMethodNotAllowed: true,
		NotFound:               http.FileServer(http.Dir(uiDir)),
		RedirectFixedPath:      true,
	}

	prefix := dsPathPrefix + "/:store"
	router.POST(prefix+constants.GetRefsPath, routeToStore(stores, datas.HandleGetRefs))
	router.OPTIONS(prefix+constants.GetRefsPath, routeToStore(stores, datas.HandleGetRefs))
	router.GET(prefix+constants.RootPath, routeToStore(stores, datas.HandleRootGet))
	router.POST(prefix+constants.RootPath, routeToStore(stores, datas.HandleRootPost))
	router.OPTIONS(prefix+constants.RootPath, routeToStore(stores, datas.HandleRootGet))

	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *portFlag))
	d.Chk.NoError(err)

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			router.ServeHTTP(w, req)
		}),
	}

	qs := ""
	if len(qsValues) > 0 {
		qs = "?" + qsValues.Encode()
	}

	fmt.Printf("Starting UI %s at http://%s%s\n", uiDir, l.Addr().String(), qs)
	log.Fatal(srv.Serve(l))
}

func constructQueryString(args []string) (url.Values, chunkStoreRecords) {
	qsValues := url.Values{}
	stores := chunkStoreRecords{}

	for _, arg := range args {
		k, v, ok := split2(arg, "=")
		if !ok {
			continue
		}

		// Magically assume that ldb: prefixed arguments are references to ldb stores. If so, construct
		// httpstore proxies to them, and rewrite the path to the client.
		// TODO: When clients can declare a nomdl interface, this can be much stricter. There should be
		// no need to search and attempt to string match every argument.
		if strings.HasPrefix(v, "ldb:") {
			_, path, _ := split2(v, ":")
			record, ok := stores[path]
			if !ok {
				record.cs = chunks.NewLevelDBStoreUseFlags(path, "")
				// Identify the stores with a (abridged) hash of the file system path,
				// so that the same URL always refers to the same database.
				hash := sha1.Sum([]byte(path))
				record.alias = hex.EncodeToString(hash[:])[:8]
				stores[path] = record
			}
			v = fmt.Sprintf("%s/%s", dsPathPrefix, record.alias)
		}

		qsValues.Add(k, v)
	}

	return qsValues, stores
}

func routeToStore(stores chunkStoreRecords, handler datas.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		store := params.ByName("store")
		for _, record := range stores {
			if record.alias == store {
				handler(w, r, params, record.cs)
				return
			}
		}
		d.Chk.Fail("No store named", store)
	}
}

func split2(s, sep string) (string, string, bool) {
	substrs := strings.SplitN(s, sep, 2)
	if len(substrs) != 2 {
		fmt.Printf("Invalid arg %s, must be of form k%sv\n", s, sep)
		return "", "", false
	}
	return substrs[0], substrs[1], true
}
