// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/sizecache"
)

type tableCache interface {
	checkout(h addr) io.ReaderAt
	checkin(h addr)
	store(h addr, data io.Reader, size uint64)
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

	ftc.init(maxOpenFds)
	return ftc
}

func (ftc *fsTableCache) init(concurrency int) {
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
				os.Remove(path)
				return nil
			}
			if !isTableFile(info) {
				return errors.New(path + " is not a table file; cache dir must contain only table files")
			}
			infos <- finfo{path, ParseAddr([]byte(info.Name())), uint64(info.Size())}
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
				ftc.fd.RefFile(info.path)
				ftc.fd.UnrefFile(info.path)
			}
		}()
	}
	wg.Wait()
	d.PanicIfError(<-errc)
}

func (ftc *fsTableCache) checkout(h addr) io.ReaderAt {
	if _, ok := ftc.cache.Get(h); !ok {
		return nil
	}

	if fd, err := ftc.fd.RefFile(filepath.Join(ftc.dir, h.String())); err == nil {
		return fd
	}
	return nil
}

func (ftc *fsTableCache) checkin(h addr) {
	ftc.fd.UnrefFile(filepath.Join(ftc.dir, h.String()))
}

func (ftc *fsTableCache) store(h addr, data io.Reader, size uint64) {
	path := filepath.Join(ftc.dir, h.String())
	tempName := func() string {
		temp, err := ioutil.TempFile(ftc.dir, tempTablePrefix)
		d.PanicIfError(err)
		defer checkClose(temp)
		io.Copy(temp, data)
		return temp.Name()
	}()

	err := os.Rename(tempName, path)
	d.PanicIfError(err)

	ftc.cache.Add(h, size, true)

	ftc.fd.RefFile(path) // Prime the file in the fd cache
	ftc.fd.UnrefFile(path)
}

func (ftc *fsTableCache) expire(h addr) {
	err := os.Remove(filepath.Join(ftc.dir, h.String()))
	d.PanicIfError(err)
}
