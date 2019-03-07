package main

import (
	"fmt"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

func ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
	logger := getReqLogger("HTTP_"+req.Method, req.URL.String())
	defer func() { logger("finished") }()

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
		statusCode = readChunk(logger, org, repo, chunk, respWr)
	case http.MethodPost:
		statusCode = writeChunk(logger, org, repo, chunk, req)
	}

	if statusCode != -1 {
		respWr.WriteHeader(statusCode)
	}
}

func writeChunk(logger func(string), org, repo, hashStr string, request *http.Request) int {
	cs, err := csCache.Get(org, repo)

	if err != nil {
		logger(err.Error())
		return http.StatusInternalServerError
	}

	logger(fmt.Sprintf("repo %s/%s is valid.", org, repo))
	h, ok := hash.MaybeParse(hashStr)

	if !ok {
		logger(hashStr + " is not a valid hash")
		return http.StatusBadRequest
	}

	logger(hashStr + " is valid")
	data, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger("failed to read body " + err.Error())
		return http.StatusInternalServerError
	}

	c := chunks.NewChunk(data)

	if c.Hash() != h {
		logger(hashStr + " does not match the hash of the data. size: " + strconv.FormatInt(int64(len(data)), 10))
		for k, v := range request.Header {
			log.Println("\t", k, ":", v)
		}

		return http.StatusBadRequest
	}

	logger(fmt.Sprintf("Received valid chunk %s of size %d", h.String(), len(data)))
	cs.Put(c)

	logger("Successfully cached data")
	return http.StatusOK
}

func readChunk(logger func(string), org, repo, hashStr string, writer io.Writer) int {
	cs, err := csCache.Get(org, repo)

	if err != nil {
		logger(err.Error())
		return http.StatusInternalServerError
	}

	logger(fmt.Sprintf("repo %s/%s is valid.", org, repo))
	h, ok := hash.MaybeParse(hashStr)

	if !ok {
		logger(hashStr + " is not a valid hash")
		return http.StatusBadRequest
	}

	logger(hashStr + " is valid")
	c := cs.Get(h)

	if c.IsEmpty() {
		logger(filepath.Join(org, repo, hashStr) + " not found")
		return http.StatusNotFound
	}

	logger(fmt.Sprintf("retrieved data for chunk %s. size: %d", hashStr, len(c.Data())))
	err = iohelp.WriteAll(writer, c.Data())

	if err != nil {
		logger("failed to write data to response " + err.Error())
	}

	logger("Successfully wrote data")
	return -1
}
