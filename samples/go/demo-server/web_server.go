// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/julienschmidt/httprouter"
)

const (
	dbParam      = "dbName"
	nomsBaseHtml = "<html><head></head><body><p>Hi. This is a Noms HTTP server.</p><p>To learn more, visit <a href=\"https://github.com/attic-labs/noms\">our GitHub project</a>.</p></body></html>"
)

var (
	authRegexp = regexp.MustCompile("^Bearer\\s+(\\S*)$")
	router     *httprouter.Router
	authKey    = ""
)

func setupWebServer(factory chunks.Factory) *httprouter.Router {
	router := &httprouter.Router{
		HandleMethodNotAllowed: true,
		NotFound:               http.HandlerFunc(notFound),
		PanicHandler:           panicHandler,
		RedirectFixedPath:      true,
	}

	// Note: We use the beginning of the url path as the database name. Consequently, these routes
	// don't match. For each request, h.NotFound() ends up getting called. That function separtes
	// the database name from the endpoint and then looks up the route and invokes its handler.
	// e.g. http://localhost:8000/dan/root/ doesn't match any of these routes. h.NotFound(), will
	// pull out "dan" and lookup up the "/root/" route, and then invoke it.

	router.GET(constants.RootPath, corsHandle(storeHandle(factory, datas.HandleRootGet)))
	router.POST(constants.RootPath, corsHandle(authorizeHandle(storeHandle(factory, datas.HandleRootPost))))
	router.OPTIONS(constants.RootPath, corsHandle(noopHandle))

	router.POST(constants.GetRefsPath, corsHandle(storeHandle(factory, datas.HandleGetRefs)))
	router.OPTIONS(constants.GetRefsPath, corsHandle(noopHandle))

	router.POST(constants.HasRefsPath, corsHandle(storeHandle(factory, datas.HandleHasRefs)))
	router.OPTIONS(constants.HasRefsPath, corsHandle(noopHandle))

	router.POST(constants.WriteValuePath, corsHandle(authorizeHandle(storeHandle(factory, datas.HandleWriteValue))))
	router.OPTIONS(constants.WriteValuePath, corsHandle(noopHandle))

	router.GET(constants.BasePath, handleBaseGet)

	return router
}

func startWebServer(factory chunks.Factory, key string) {
	d.Chk.NotEmpty(key, "No auth key was provided to startWebServer")
	authKey = key
	router = setupWebServer(factory)

	fmt.Printf("Listening on http://localhost:%d/...\n", *portFlag)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portFlag))
	d.Chk.NoError(err)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			router.ServeHTTP(w, req)
		}),
	}

	log.Fatal(srv.Serve(l))
}

// Attach handlers that provide the Database API
func storeHandle(factory chunks.Factory, hndlr datas.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		cs := factory.CreateStore(params.ByName(dbParam))
		defer cs.Close()
		hndlr(w, req, params, cs)
	}
}

func authorizeHandle(f httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		authHeader := r.Header.Get("Authorization")
		token := ""

		if authHeader != "" {
			res := authRegexp.FindStringSubmatch(authHeader)
			if res != nil {
				token = res[1]
			}
		}

		if token == "" {
			token = r.URL.Query().Get("access_token")
		}

		authorized := token == authKey
		if !authorized {
			w.Header().Set("WWW-Authenticate", "Bearer realm=\"Restricted\", error=\"invalid token\"")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		f(w, r, ps)
	}
}

func noopHandle(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
}

func corsHandle(f httprouter.Handle) httprouter.Handle {
	// TODO: Implement full pre-flighting?
	// See: http://www.html5rocks.com/static/images/cors_server_flowchart.png
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// Can't use * when clients are using cookies.
		w.Header().Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Add("Access-Control-Allow-Methods", "GET, POST")
		w.Header().Add("Access-Control-Allow-Headers", datas.NomsVersionHeader)
		w.Header().Add("Access-Control-Expose-Headers", datas.NomsVersionHeader)
		w.Header().Add(datas.NomsVersionHeader, constants.NomsVersion)
		f(w, r, ps)
	}
}

func panicHandler(w http.ResponseWriter, r *http.Request, recover interface{}) {
	fmt.Fprintf(os.Stderr, "error for request: %s\n", r.URL)
	fmt.Fprintf(os.Stderr, "server error: %s\n", recover)
	debug.PrintStack()
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func notFound(w http.ResponseWriter, r *http.Request) {
	u := r.URL
	p := u.Path
	route := "/" + path.Base(p) + "/"
	databaseId := path.Dir(strings.TrimRight(p, "/"))
	hndl, params, _ := router.Lookup(r.Method, route)
	if hndl == nil {
		http.NotFound(w, r)
		return
	}
	newParams := append(httprouter.Params{}, httprouter.Param{Key: dbParam, Value: databaseId})
	newParams = append(newParams, params...)
	hndl(w, r, newParams)
}

func handleBaseGet(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	d.PanicIfTrue(req.Method != "GET", "Expected get method.")

	w.Header().Add("content-type", "text/html")
	fmt.Fprintf(w, nomsBaseHtml)
}
