// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	gohash "hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var (
	ErrReadOutOfBounds = errors.New("cannot read file for given length and " +
		"offset since the read would exceed the size of the file")
)

type fileDetails struct {
	details *sync.Map
}

func (fd fileDetails) Put(id string, tfd *remotesapi.TableFileDetails) {
	fd.details.Store(id, tfd)
}

func (fd fileDetails) Get(id string) (*remotesapi.TableFileDetails, bool) {
	v, ok := fd.details.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*remotesapi.TableFileDetails), true
}

func newFileDetails() fileDetails {
	return fileDetails{new(sync.Map)}
}

type filehandler struct {
	dbCache       *DBCache
	expectedFiles fileDetails
}

func (fh filehandler) ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
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
		statusCode = readTableFile(logger, org, repo, hashStr, respWr, req)

	case http.MethodPost, http.MethodPut:
		statusCode = writeTableFile(req.Context(), logger, fh.dbCache, fh.expectedFiles, org, repo, hashStr, req)
	}

	if statusCode != -1 {
		respWr.WriteHeader(statusCode)
	}
}

func readTableFile(logger func(string), org, repo, fileId string, respWr http.ResponseWriter, req *http.Request) int {
	rangeStr := req.Header.Get("Range")
	path := filepath.Join(org, repo, fileId)

	var r io.ReadCloser
	var readSize int64
	var fileErr error
	{
		if rangeStr == "" {
			logger("going to read entire file")
			r, readSize, fileErr = getFileReader(path)
		} else {
			offset, length, err := offsetAndLenFromRange(rangeStr)
			if err != nil {
				logger(err.Error())
				return http.StatusBadRequest
			}
			logger(fmt.Sprintf("going to read file at offset %d, length %d", offset, length))
			readSize = length
			r, fileErr = getFileReaderAt(path, offset, length)
		}
	}
	if fileErr != nil {
		logger(fileErr.Error())
		if errors.Is(fileErr, os.ErrNotExist) {
			return http.StatusNotFound
		} else if errors.Is(fileErr, ErrReadOutOfBounds) {
			return http.StatusBadRequest
		}
		return http.StatusInternalServerError
	}
	defer func() {
		err := r.Close()
		if err != nil {
			err = fmt.Errorf("failed to close file at path %s: %w", path, err)
			logger(err.Error())
		}
	}()

	logger(fmt.Sprintf("opened file at path %s, going to read %d bytes", path, readSize))

	n, err := io.Copy(respWr, r)
	if err != nil {
		err = fmt.Errorf("failed to write data to response writer: %w", err)
		logger(err.Error())
		return http.StatusInternalServerError
	}
	if n != readSize {
		logger(fmt.Sprintf("wanted to write %d bytes from file (%s) but only wrote %d", readSize, path, n))
		return http.StatusInternalServerError
	}

	logger(fmt.Sprintf("wrote %d bytes", n))

	return http.StatusOK
}

type uploadreader struct {
	r            io.ReadCloser
	totalread    int
	expectedread uint64
	expectedsum  []byte
	checksum     gohash.Hash
}

func (u *uploadreader) Read(p []byte) (n int, err error) {
	n, err = u.r.Read(p)
	if err == nil || err == io.EOF {
		u.totalread += n
		u.checksum.Write(p[:n])
	}
	return n, err
}

var errBodyLengthTFDMismatch = errors.New("body upload length did not match table file details")
var errBodyHashTFDMismatch = errors.New("body upload hash did not match table file details")

func (u *uploadreader) Close() error {
	cerr := u.r.Close()
	if cerr != nil {
		return cerr
	}
	if u.expectedread != 0 && u.expectedread != uint64(u.totalread) {
		return errBodyLengthTFDMismatch
	}
	sum := u.checksum.Sum(nil)
	if !bytes.Equal(u.expectedsum, sum[:]) {
		return errBodyHashTFDMismatch
	}
	return nil
}

func writeTableFile(ctx context.Context, logger func(string), dbCache *DBCache, expectedFiles fileDetails, org, repo, fileId string, request *http.Request) int {
	_, ok := hash.MaybeParse(fileId)

	if !ok {
		logger(fileId + " is not a valid hash")
		return http.StatusBadRequest
	}

	tfd, ok := expectedFiles.Get(fileId)
	if !ok {
		logger("bad request for " + fileId + ": tfd not found")
		return http.StatusBadRequest
	}

	logger(fileId + " is valid")

	cs, err := dbCache.Get(org, repo, types.Format_Default.VersionString())
	if err != nil {
		logger("failed to get " + org + "/" + repo + " repository: " + err.Error())
		return http.StatusInternalServerError
	}

	err = cs.WriteTableFile(ctx, fileId, int(tfd.NumChunks), tfd.ContentHash, func() (io.ReadCloser, uint64, error) {
		reader := request.Body
		size := tfd.ContentLength
		return &uploadreader{
			reader,
			0,
			tfd.ContentLength,
			tfd.ContentHash,
			md5.New(),
		}, size, nil
	})

	if err != nil {
		if errors.Is(err, errBodyLengthTFDMismatch) {
			logger("bad write file request for " + fileId + ": body length mismatch")
			return http.StatusBadRequest
		}
		if errors.Is(err, errBodyHashTFDMismatch) {
			logger("bad write file request for " + fileId + ": body hash mismatch")
			return http.StatusBadRequest
		}
		logger("failed to read body " + err.Error())
		return http.StatusInternalServerError
	}

	return http.StatusOK
}

func writeLocal(logger func(string), org, repo, fileId string, data []byte) error {
	path := filepath.Join(org, repo, fileId)

	err := os.WriteFile(path, data, os.ModePerm)

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

// getFileReader opens a file at the given path and returns an io.ReadCloser,
// the corresponding file's filesize, and a http status.
func getFileReader(path string) (io.ReadCloser, int64, error) {
	return openFile(path)
}

func openFile(path string) (*os.File, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get stats for file at path %s: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file at path %s: %w", path, err)
	}

	return f, info.Size(), nil
}

type closerReaderWrapper struct {
	io.Reader
	io.Closer
}

func getFileReaderAt(path string, offset int64, length int64) (io.ReadCloser, error) {
	f, fSize, err := openFile(path)
	if err != nil {
		return nil, err
	}

	if fSize < int64(offset+length) {
		return nil, fmt.Errorf("failed to read file %s at offset %d, length %d: %w", path, offset, length, ErrReadOutOfBounds)
	}

	_, err = f.Seek(int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek file at path %s to offset %d: %w", path, offset, err)
	}

	r := closerReaderWrapper{io.LimitReader(f, length), f}
	return r, nil
}
