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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/store/util/sizecache"
)

type tableCache interface {
	checkout(h addr) (io.ReaderAt, error)
	checkin(h addr) error
	store(h addr, data io.Reader, size uint64) error
}

type fsTableCache struct {
	dir   string
	cache *sizecache.SizeCache
	fd    *fdCache
}

func newFSTableCache(dir string, cacheSize uint64, maxOpenFds int) (*fsTableCache, error) {
	ftc := &fsTableCache{dir: dir, fd: newFDCache(maxOpenFds)}
	ftc.cache = sizecache.NewWithExpireCallback(cacheSize, func(elm interface{}) {
		ftc.expire(elm.(addr))
	})

	err := ftc.init(maxOpenFds)

	if err != nil {
		return nil, err
	}

	return ftc, nil
}

func (ftc *fsTableCache) init(concurrency int) error {
	type finfo struct {
		path string
		h    addr
		size uint64
	}
	infos := make(chan finfo)
	errc := make(chan error, 1)
	go func() {
		isTableFile := func(info os.FileInfo) bool {
			return info.Mode().IsRegular() && ValidateAddr(info.Name())
		}
		isTempTableFile := func(info os.FileInfo) bool {
			return info.Mode().IsRegular() && strings.HasPrefix(info.Name(), tempTablePrefix)
		}
		defer close(errc)
		defer close(infos)
		// No select needed for this send, since errc is buffered.
		errc <- filepath.Walk(ftc.dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path == ftc.dir {
				return nil
			}
			if isTempTableFile(info) {
				// ignore failure to remove temp file
				_ = os.Remove(path)
				return nil
			}
			if !isTableFile(info) {
				return errors.New(path + " is not a table file; cache dir must contain only table files")
			}

			ad, err := parseAddr([]byte(info.Name()))

			if err != nil {
				return err
			}

			infos <- finfo{path, ad, uint64(info.Size())}
			return nil
		})
	}()

	ae := atomicerr.New()
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for info := range infos {
				if ae.IsSet() {
					break
				}

				ftc.cache.Add(info.h, info.size, true)
				_, err := ftc.fd.RefFile(info.path)

				if err != nil {
					ae.SetIfError(err)
					break
				}

				err = ftc.fd.UnrefFile(info.path)

				if err != nil {
					ae.SetIfError(err)
					break
				}
			}
		}()
	}
	wg.Wait()

	err := <-errc

	if err != nil {
		return err
	}

	if err := ae.Get(); err != nil {
		return err
	}

	return nil
}

func (ftc *fsTableCache) checkout(h addr) (io.ReaderAt, error) {
	if _, ok := ftc.cache.Get(h); !ok {
		return nil, nil
	}

	fd, err := ftc.fd.RefFile(filepath.Join(ftc.dir, h.String()))

	if err != nil {
		return nil, err
	}

	return fd, nil
}

func (ftc *fsTableCache) checkin(h addr) error {
	return ftc.fd.UnrefFile(filepath.Join(ftc.dir, h.String()))
}

func (ftc *fsTableCache) store(h addr, data io.Reader, size uint64) error {
	path := filepath.Join(ftc.dir, h.String())
	tempName, err := func() (name string, ferr error) {
		var temp *os.File
		temp, ferr = MovableTempFile.NewFile(ftc.dir, tempTablePrefix)

		if ferr != nil {
			return "", ferr
		}

		defer func() {
			closeErr := temp.Close()

			if ferr == nil {
				ferr = closeErr
			}
		}()

		_, ferr = io.Copy(temp, data)

		if ferr != nil {
			return "", ferr
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return err
	}

	err = ftc.fd.ShrinkCache()

	if err != nil {
		return err
	}

	err = os.Rename(tempName, path)

	if err != nil {
		return err
	}

	ftc.cache.Add(h, size, true)

	// Prime the file in the fd cache ignore err
	if _, err = ftc.fd.RefFile(path); err == nil {
		err := ftc.fd.UnrefFile(path)

		if err != nil {
			return err
		}
	}

	return nil
}

func (ftc *fsTableCache) expire(h addr) error {
	return os.Remove(filepath.Join(ftc.dir, h.String()))
}
