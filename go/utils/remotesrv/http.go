package main

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strings"
)

func ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
	logFinish := logStart(req.Method + ":" + req.URL.Path)
	defer logFinish()

	path := strings.TrimLeft(req.URL.Path, "/")
	tokens := strings.Split(path, "/")

	if len(tokens) != 3 {
		log.Println("response to:", req.URL, "method:", req.Method, "http response code: ", http.StatusNotFound)
		respWr.WriteHeader(http.StatusNotFound)
		return
	}

	org := tokens[0]
	repo := tokens[1]
	chunk := tokens[2]

	statusCode := http.StatusMethodNotAllowed
	switch req.Method {
	case http.MethodGet:
		statusCode = readChunk(org, repo, chunk, respWr)
	case http.MethodPost:
		statusCode = writeChunk(org, repo, chunk, req)
	}

	respWr.WriteHeader(statusCode)
}

func writeChunk(org, repo, hashStr string, request *http.Request) int {
	cs, err := csCache.Get(org, repo)

	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}

	h, ok := hash.MaybeParse(hashStr)

	if !ok {
		log.Println(hashStr, "is not a valid hash")
		return http.StatusBadRequest
	}

	data, err := ioutil.ReadAll(request.Body)

	if err != nil {
		log.Println("failed to read body", err)
		return http.StatusInternalServerError
	}

	c := chunks.NewChunk(data)

	if c.Hash() != h {
		log.Println(hashStr, "does not match the hash of the data. size:", len(data))
		for k, v := range request.Header {
			log.Println("\t", k, ":", v)
		}

		return http.StatusBadRequest
	}

	log.Println("Received valid chunk", h.String())
	cs.Put(c)

	return http.StatusOK
}

func readChunk(org, repo, hashStr string, writer io.Writer) int {
	cs, err := csCache.Get(org, repo)

	if err != nil {
		log.Println(err)
		return http.StatusInternalServerError
	}

	h, ok := hash.MaybeParse(hashStr)

	if !ok {
		log.Println(hashStr, "is not a valid hash")
		return http.StatusBadRequest
	}

	c := cs.Get(h)

	if c.IsEmpty() {
		log.Println(filepath.Join(org, repo, hashStr, "not found"))
		return http.StatusNotFound
	}

	err = iohelp.WriteAll(writer, c.Data())

	if err != nil {
		log.Println("failed to write data to response", err)
		return http.StatusInternalServerError
	}

	return http.StatusOK
}
