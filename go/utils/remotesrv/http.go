// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
	logger := getReqLogger("HTTP_"+req.Method, req.RequestURI)
	defer func() { logger("finished") }()

	path := strings.TrimLeft(req.URL.Path, "/")
	tokens := strings.Split(path, "/")

	if len(tokens) != 3 {
		logger(fmt.Sprintf("response to: %v method: %v http response code: %v", req.RequestURI, req.Method, http.StatusNotFound))
		respWr.WriteHeader(http.StatusNotFound)
	}

	org := tokens[0]
	repo := tokens[1]
	hashStr := tokens[2]

	statusCode := http.StatusMethodNotAllowed
	switch req.Method {
	case http.MethodGet:
		rangeStr := req.Header.Get("Range")

		if rangeStr == "" {
			statusCode = readFile(logger, org, repo, hashStr, respWr)
		} else {
			statusCode = readChunk(logger, org, repo, hashStr, rangeStr, respWr)
		}

	case http.MethodPost, http.MethodPut:
		statusCode = writeChunk(logger, org, repo, hashStr, req)
	}

	if statusCode != -1 {
		respWr.WriteHeader(statusCode)
	}
}

func writeChunk(logger func(string), org, repo, fileId string, request *http.Request) int {
	_, ok := hash.MaybeParse(fileId)

	if !ok {
		logger(fileId + " is not a valid hash")
		return http.StatusBadRequest
	}

	logger(fileId + " is valid")
	data, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger("failed to read body " + err.Error())
		return http.StatusInternalServerError
	}

	err = writeLocal(logger, org, repo, fileId, data)

	if err != nil {
		return http.StatusInternalServerError
	}

	return http.StatusOK
}

func writeLocal(logger func(string), org, repo, fileId string, data []byte) error {
	path := filepath.Join(org, repo, fileId)

	err := ioutil.WriteFile(path, data, os.ModePerm)

	if err != nil {
		logger(fmt.Sprintf("failed to write file %s", path))
		return err
	}

	logger("Successfully wrote object to storage")

	return nil
}

func offsetAndLenFromRange(rngStr string) (int64, int64, error) {
	if rngStr == "" {
		return -1, -1, nil
	}

	if !strings.HasPrefix(rngStr, "bytes=") {
		return -1, -1, errors.New("range string does not start with 'bytes=")
	}

	tokens := strings.Split(rngStr[6:], "-")

	if len(tokens) != 2 {
		return -1, -1, errors.New("invalid range format. should be bytes=#-#")
	}

	start, err := strconv.ParseUint(strings.TrimSpace(tokens[0]), 10, 64)

	if err != nil {
		return -1, -1, errors.New("invalid offset is not a number. should be bytes=#-#")
	}

	end, err := strconv.ParseUint(strings.TrimSpace(tokens[1]), 10, 64)

	if err != nil {
		return -1, -1, errors.New("invalid length is not a number. should be bytes=#-#")
	}

	return int64(start), int64(end-start) + 1, nil
}


func readFile(logger func(string), org, repo, fileId string, writer io.Writer) int {
	path := filepath.Join(org, repo, fileId)

	info, err := os.Stat(path)

	if err != nil {
		logger("file not found. path: " + path)
		return http.StatusNotFound
	}

	f, err := os.Open(path)

	if err != nil {
		logger("failed to open file. file: " + path + " err: " + err.Error())
		return http.StatusInternalServerError
	}

	n, err := io.Copy(writer, f)

	if err != nil {
		logger("failed to write data to response. err : " + err.Error())
		return -1
	}

	if n != info.Size() {
		logger(fmt.Sprint("failed to write entire file to response. Copied %d of %d err: %v", n, info.Size(), err))
		return -1
	}

	return -1
}

func readChunk(logger func(string), org, repo, fileId, rngStr string, writer io.Writer) int {
	offset, length, err := offsetAndLenFromRange(rngStr)

	if err != nil {
		logger(fmt.Sprintln(rngStr, "is not a valid range"))
		return http.StatusBadRequest
	}

	data, retVal := readLocalRange(logger, org, repo, fileId, int64(offset), int64(length))

	if retVal != -1 {
		return retVal
	}

	logger(fmt.Sprintf("writing %d bytes", len(data)))
	err = iohelp.WriteAll(writer, data)

	if err != nil {
		logger("failed to write data to response " + err.Error())
		return -1
	}

	logger("Successfully wrote data")
	return -1
}

func readLocalRange(logger func(string), org, repo, fileId string, offset, length int64) ([]byte, int) {
	path := filepath.Join(org, repo, fileId)

	logger(fmt.Sprintf("Attempting to read bytes %d to %d from %s", offset, offset+length, path))
	info, err := os.Stat(path)

	if err != nil {
		logger(fmt.Sprintf("file %s not found", path))
		return nil, http.StatusNotFound
	}

	logger(fmt.Sprintf("Verified file %s exists", path))

	if info.Size() < int64(offset+length) {
		logger(fmt.Sprintf("Attempted to read bytes %d to %d, but the file is only %d bytes in size", offset, offset+length, info.Size()))
		return nil, http.StatusBadRequest
	}

	logger(fmt.Sprintf("Verified the file is large enough to contain the range"))
	f, err := os.Open(path)

	if err != nil {
		logger(fmt.Sprintf("Failed to open %s", path))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Successfully opened file"))
	pos, err := f.Seek(int64(offset), 0)

	if err != nil {
		logger(fmt.Sprintf("Failed to seek to %d", offset))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Seek succeeded.  Current position is %d", pos))
	diff := int64(offset) - pos
	data, err := iohelp.ReadNBytes(f, int(diff+int64(length)))

	if err != nil {
		logger(fmt.Sprintf("Failed to read %d bytes", diff+int64(length)))
		return nil, http.StatusInternalServerError
	}

	logger(fmt.Sprintf("Successfully read %d bytes", len(data)))
	return data[diff:], -1
}
