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

package remotesrv

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	gohash "hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var (
	ErrReadOutOfBounds = errors.New("cannot read file for given length and " +
		"offset since the read would exceed the size of the file")
)

type filehandler struct {
	dbCache  DBCache
	fs       filesys.Filesys
	readOnly bool
	lgr      *logrus.Entry
	sealer   Sealer
}

func newFileHandler(lgr *logrus.Entry, dbCache DBCache, fs filesys.Filesys, readOnly bool, sealer Sealer) filehandler {
	return filehandler{
		dbCache,
		fs,
		readOnly,
		lgr.WithFields(logrus.Fields{
			"service": "dolt.services.remotesapi.v1alpha1.HttpFileServer",
		}),
		sealer,
	}
}

func (fh filehandler) ServeHTTP(respWr http.ResponseWriter, req *http.Request) {
	logger := getReqLogger(fh.lgr, req.Method+"_"+req.RequestURI)
	defer func() { logger.Info("finished") }()

	var err error
	req.URL, err = fh.sealer.Unseal(req.URL)
	if err != nil {
		logger.WithError(err).Warn("could not unseal incoming request URL")
		respWr.WriteHeader(http.StatusBadRequest)
		return
	}

	logger = logger.WithField("unsealed_url", req.URL.String())

	path := strings.TrimLeft(req.URL.Path, "/")

	statusCode := http.StatusMethodNotAllowed
	switch req.Method {
	case http.MethodGet:
		path = filepath.Clean(path)
		if strings.HasPrefix(path, "../") || strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") {
			logger.Warn("bad request with .. in URL path")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		i := strings.LastIndex(path, "/")
		if i == -1 {
			logger.Warn("bad request with -1 LastIndex of '/' for path")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		_, ok := hash.MaybeParse(path[i+1:])
		if !ok {
			logger.WithField("last_path_component", path[i+1:]).Warn("bad request with unparsable last path component")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		abs, err := fh.fs.Abs(path)
		if err != nil {
			logger.WithError(err).Error("could not get absolute path")
			respWr.WriteHeader(http.StatusInternalServerError)
			return
		}
		respWr.Header().Add("Accept-Ranges", "bytes")
		logger, statusCode = readTableFile(logger, abs, respWr, req.Header.Get("Range"))

	case http.MethodPost, http.MethodPut:
		if fh.readOnly {
			respWr.WriteHeader(http.StatusForbidden)
			return
		}

		i := strings.LastIndex(path, "/")
		// a table file name is currently 32 characters, plus the '/' is 33.
		if i < 0 || len(path[i:]) != 33 {
			logger = logger.WithField("status", http.StatusNotFound)
			respWr.WriteHeader(http.StatusNotFound)
			return
		}

		filepath := path[:i]
		file := path[i+1:]

		q := req.URL.Query()
		ncs := q.Get("num_chunks")
		if ncs == "" {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: num_chunks parameter not provided")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		num_chunks, err := strconv.Atoi(ncs)
		if err != nil {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.WithError(err).Warn("bad request: num_chunks parameter did not parse")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		cls := q.Get("content_length")
		if cls == "" {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: content_length parameter not provided")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		content_length, err := strconv.Atoi(cls)
		if err != nil {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.WithError(err).Warn("bad request: content_length parameter did not parse")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		chs := q.Get("content_hash")
		if chs == "" {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: content_hash parameter not provided")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}
		content_hash, err := base64.RawURLEncoding.DecodeString(chs)
		if err != nil {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.WithError(err).Warn("bad request: content_hash parameter did not parse")
			respWr.WriteHeader(http.StatusBadRequest)
			return
		}

		logger, statusCode = writeTableFile(req.Context(), logger, fh.dbCache, filepath, file, num_chunks, content_hash, uint64(content_length), req.Body)
	}

	if statusCode != -1 {
		respWr.WriteHeader(statusCode)
	}
}

func readTableFile(logger *logrus.Entry, path string, respWr http.ResponseWriter, rangeStr string) (*logrus.Entry, int) {
	var r io.ReadCloser
	var readSize int64
	var fileErr error
	{
		if rangeStr == "" {
			logger = logger.WithField("whole_file", true)
			r, readSize, fileErr = getFileReader(path)
		} else {
			offset, length, headerStr, err := offsetAndLenFromRange(rangeStr)
			if err != nil {
				logger.Println(err.Error())
				return logger, http.StatusBadRequest
			}
			logger = logger.WithFields(logrus.Fields{
				"read_offset": offset,
				"read_length": length,
			})
			readSize = length
			var fSize int64
			r, fSize, fileErr = getFileReaderAt(path, offset, length)
			if fileErr == nil {
				respWr.Header().Add("Content-Range", headerStr+strconv.Itoa(int(fSize)))
			}
		}
	}
	if fileErr != nil {
		logger.Println(fileErr.Error())
		if errors.Is(fileErr, os.ErrNotExist) {
			logger = logger.WithField("status", http.StatusNotFound)
			return logger, http.StatusNotFound
		} else if errors.Is(fileErr, ErrReadOutOfBounds) {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: offset out of bounds for path")
			return logger, http.StatusBadRequest
		}
		logger = logger.WithError(fileErr)
		return logger, http.StatusInternalServerError
	}
	defer func() {
		err := r.Close()
		if err != nil {
			logger.WithError(err).Warn("failed to close file")
		}
	}()

	if rangeStr == "" {
		respWr.WriteHeader(http.StatusPartialContent)
	} else {
		respWr.WriteHeader(http.StatusOK)
	}

	n, err := io.Copy(respWr, r)
	if err != nil {
		logger = logger.WithField("status", http.StatusInternalServerError)
		logger.WithError(err).Error("error copying data to response writer")
		return logger, http.StatusInternalServerError
	}
	if n != readSize {
		logger = logger.WithField("status", http.StatusInternalServerError)
		logger.WithField("copied_size", n).Error("failed to copy all bytes to response")
		return logger, http.StatusInternalServerError
	}

	return logger, -1
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

func writeTableFile(ctx context.Context, logger *logrus.Entry, dbCache DBCache, path, fileId string, numChunks int, contentHash []byte, contentLength uint64, body io.ReadCloser) (*logrus.Entry, int) {
	_, ok := hash.MaybeParse(fileId)
	if !ok {
		logger = logger.WithField("status", http.StatusBadRequest)
		logger.Warnf("%s is not a valid hash", fileId)
		return logger, http.StatusBadRequest
	}

	cs, err := dbCache.Get(ctx, path, types.Format_Default.VersionString())
	if err != nil {
		logger = logger.WithField("status", http.StatusInternalServerError)
		logger.WithError(err).Error("failed to get repository")
		return logger, http.StatusInternalServerError
	}

	err = cs.WriteTableFile(ctx, fileId, numChunks, contentHash, func() (io.ReadCloser, uint64, error) {
		reader := body
		size := contentLength
		return &uploadreader{
			reader,
			0,
			contentLength,
			contentHash,
			md5.New(),
		}, size, nil
	})

	if err != nil {
		if errors.Is(err, errBodyLengthTFDMismatch) {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: body length mismatch")
			return logger, http.StatusBadRequest
		}
		if errors.Is(err, errBodyHashTFDMismatch) {
			logger = logger.WithField("status", http.StatusBadRequest)
			logger.Warn("bad request: body hash mismatch")
			return logger, http.StatusBadRequest
		}
		logger = logger.WithField("status", http.StatusInternalServerError)
		logger.WithError(err).Error("failed to write upload to table file")
		return logger, http.StatusInternalServerError
	}

	return logger, http.StatusOK
}

func offsetAndLenFromRange(rngStr string) (int64, int64, string, error) {
	if rngStr == "" {
		return -1, -1, "", nil
	}

	if !strings.HasPrefix(rngStr, "bytes=") {
		return -1, -1, "", errors.New("range string does not start with 'bytes=")
	}

	tokens := strings.Split(rngStr[6:], "-")

	if len(tokens) != 2 {
		return -1, -1, "", errors.New("invalid range format. should be bytes=#-#")
	}

	start, err := strconv.ParseUint(strings.TrimSpace(tokens[0]), 10, 64)

	if err != nil {
		return -1, -1, "", errors.New("invalid offset is not a number. should be bytes=#-#")
	}

	end, err := strconv.ParseUint(strings.TrimSpace(tokens[1]), 10, 64)

	if err != nil {
		return -1, -1, "", errors.New("invalid length is not a number. should be bytes=#-#")
	}

	return int64(start), int64(end-start) + 1, "bytes " + tokens[0] + "-" + tokens[1] + "/", nil
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

func getFileReaderAt(path string, offset int64, length int64) (io.ReadCloser, int64, error) {
	f, fSize, err := openFile(path)
	if err != nil {
		return nil, 0, err
	}

	if fSize < int64(offset+length) {
		return nil, 0, fmt.Errorf("failed to read file %s at offset %d, length %d: %w", path, offset, length, ErrReadOutOfBounds)
	}

	_, err = f.Seek(int64(offset), 0)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to seek file at path %s to offset %d: %w", path, offset, err)
	}

	r := closerReaderWrapper{io.LimitReader(f, length), f}
	return r, fSize, nil
}

// ExtractBasicAuthCreds extracts the username and password from the incoming request. It returns RequestCredentials
// populated with necessary information to authenticate the request. nil and an error will be returned if any error
// occurs.
func ExtractBasicAuthCreds(ctx context.Context) (*RequestCredentials, error) {
	if md, ok := metadata.FromIncomingContext(ctx); !ok {
		return nil, errors.New("no metadata in context")
	} else {
		var username string
		var password string

		auths := md.Get("authorization")
		if len(auths) != 1 {
			username = "root"
			password = ""
		} else {
			auth := auths[0]
			if !strings.HasPrefix(auth, "Basic ") {
				return nil, fmt.Errorf("bad request: authorization header did not start with 'Basic '")
			}
			authTrim := strings.TrimPrefix(auth, "Basic ")
			uDec, err := base64.URLEncoding.DecodeString(authTrim)
			if err != nil {
				return nil, fmt.Errorf("incoming request authorization header failed to decode: %v", err)
			}
			userPass := strings.Split(string(uDec), ":")
			username = userPass[0]
			password = userPass[1]
		}
		addr, ok := peer.FromContext(ctx)
		if !ok {
			return nil, errors.New("incoming request had no peer")
		}

		return &RequestCredentials{Username: username, Password: password, Address: addr.Addr.String()}, nil
	}
}
