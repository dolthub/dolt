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

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/liquidata-inc/dolt/go/store/util/tempfiles"

	"github.com/google/uuid"
	"github.com/juju/fslock"
)

const (
	lfsVerSize = 16
	bsExt      = ".bs"
	lockExt    = ".lock"
)

type localBlobRangeReadCloser struct {
	br  BlobRange
	rc  io.ReadCloser
	pos int64
}

func (lbrrc *localBlobRangeReadCloser) Read(p []byte) (int, error) {
	remaining := lbrrc.br.length - lbrrc.pos

	if remaining == 0 {
		return 0, io.EOF
	} else if int64(len(p)) > remaining {
		partial := p[:remaining]
		n, err := lbrrc.rc.Read(partial)
		lbrrc.pos += int64(n)

		return n, err
	}

	n, err := lbrrc.rc.Read(p)
	lbrrc.pos += int64(n)

	return n, err
}

func (lbrrc *localBlobRangeReadCloser) Close() error {
	return lbrrc.rc.Close()
}

// LocalBlobstore is a Blobstore implementation that uses the local filesystem
type LocalBlobstore struct {
	RootDir string
}

// NewLocalBlobstore returns a new LocalBlobstore instance
func NewLocalBlobstore(dir string) *LocalBlobstore {
	return &LocalBlobstore{dir}
}

// Get retrieves an io.reader for the portion of a blob specified by br along with
// its version
func (bs *LocalBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, string, error) {
	path := filepath.Join(bs.RootDir, key) + bsExt
	f, err := os.Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", NotFound{key}
		}

		return nil, "", err
	}

	var ver uuid.UUID
	n, err := f.Read(ver[:lfsVerSize])

	if n != lfsVerSize {
		f.Close()
		return nil, "", errors.New("failed to read version")
	} else if err != nil {
		f.Close()
		return nil, "", err
	}

	rc, err := readCloserForFileRange(f, lfsVerSize, br)

	if err != nil {
		f.Close()
		return nil, "", err
	}

	return rc, ver.String(), nil
}

func readCloserForFileRange(f *os.File, pos int, br BlobRange) (io.ReadCloser, error) {
	seekType := 1
	if br.offset < 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		seekType = 0
		br = br.positiveRange(info.Size())
	}

	_, err := f.Seek(br.offset, seekType)

	if err != nil {
		return nil, err
	}

	if br.length != 0 {
		return &localBlobRangeReadCloser{br, f, 0}, nil
	}

	return f, nil
}

func writeAll(f *os.File, readers ...io.Reader) error {
	for _, reader := range readers {
		_, err := io.Copy(f, reader)

		if err != nil {
			return err
		}
	}

	return nil
}

// Put sets the blob and the version for a key
func (bs *LocalBlobstore) Put(ctx context.Context, key string, reader io.Reader) (string, error) {
	ver := uuid.New()

	// written as temp file and renamed so the file corresponding to this key
	// never exists in a partially written state
	tempFile, err := func() (string, error) {
		temp, err := tempfiles.MovableTempFileProvider.NewFile("", ver.String())

		if err != nil {
			return "", err
		}

		defer temp.Close()

		verBytes := [lfsVerSize]byte(ver)
		verReader := bytes.NewReader(verBytes[:lfsVerSize])
		return temp.Name(), writeAll(temp, verReader, reader)
	}()

	if err != nil {
		return "", err
	}

	path := filepath.Join(bs.RootDir, key) + bsExt
	err = os.Rename(tempFile, path)

	if err != nil {
		return "", err
	}

	return ver.String(), nil
}

func fLock(lockFilePath string) (*fslock.Lock, error) {
	lck := fslock.New(lockFilePath)
	err := lck.Lock()

	if err != nil {
		return nil, err
	}

	return lck, nil
}

// CheckAndPut will check the current version of a blob against an expectedVersion, and if the
// versions match it will update the data and version associated with the key
func (bs *LocalBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, reader io.Reader) (string, error) {
	path := filepath.Join(bs.RootDir, key) + bsExt
	lockFilePath := path + lockExt
	lck, err := fLock(lockFilePath)

	if err != nil {
		return "", errors.New("Could not acquire lock of " + lockFilePath)
	}

	defer lck.Unlock()

	rc, ver, err := bs.Get(ctx, key, BlobRange{})

	if err != nil {
		if !IsNotFoundError(err) {
			return "", errors.New("Unable to read current version of " + path)
		}
	} else {
		rc.Close()
	}

	if expectedVersion != ver {
		return "", CheckAndPutError{key, expectedVersion, ver}
	}

	return bs.Put(ctx, key, reader)
}

// Exists returns true if a blob exists for the given key, and false if it does not.
// error may be returned if there are errors accessing the filesystem data.
func (bs *LocalBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	path := filepath.Join(bs.RootDir, key) + bsExt
	_, err := os.Stat(path)

	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}
