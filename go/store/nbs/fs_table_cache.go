// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/store/util/sizecache"
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

func newFSTableCache(dir string, cacheSize uint64, maxOpenFds int) *fsTableCache {
	ftc := &fsTableCache{dir: dir, fd: newFDCache(maxOpenFds)}
	ftc.cache = sizecache.NewWithExpireCallback(cacheSize, func(elm interface{}) {
		ftc.expire(elm.(addr))
	})

	err := ftc.init(maxOpenFds)

	// TODO: fix panics
	d.PanicIfError(err)

	return ftc
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

			ad, err := ParseAddr([]byte(info.Name()))

			if err != nil {
				return err
			}

			infos <- finfo{path, ad, uint64(info.Size())}
			return nil
		})
	}()

	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for info := range infos {
				ftc.cache.Add(info.h, info.size, true)

				if _, err := ftc.fd.RefFile(info.path); err == nil {
					ftc.fd.UnrefFile(info.path)
				}
			}
		}()
	}
	wg.Wait()
	return <-errc
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
	ftc.fd.UnrefFile(filepath.Join(ftc.dir, h.String()))
	return nil
}

func (ftc *fsTableCache) store(h addr, data io.Reader, size uint64) error {
	path := filepath.Join(ftc.dir, h.String())
	tempName, err := func() (string, error) {
		temp, err := ioutil.TempFile(ftc.dir, tempTablePrefix)

		if err != nil {
			return "", err
		}

		defer checkClose(temp)
		_, err = io.Copy(temp, data)

		if err != nil {
			return "", err
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return err
	}

	ftc.fd.ShrinkCache()

	err = os.Rename(tempName, path)

	if err != nil {
		return err
	}

	ftc.cache.Add(h, size, true)

	// Prime the file in the fd cache ignore err
	if _, err = ftc.fd.RefFile(path); err == nil {
		ftc.fd.UnrefFile(path)
	}

	return nil
}

func (ftc *fsTableCache) expire(h addr) error {
	return os.Remove(filepath.Join(ftc.dir, h.String()))
}
