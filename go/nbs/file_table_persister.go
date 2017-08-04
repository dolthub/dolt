// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/go/d"
)

const tempTablePrefix = "nbs_table_"

func newFSTablePersister(dir string, fc *fdCache, indexCache *indexCache) tablePersister {
	d.PanicIfTrue(fc == nil)
	return &fsTablePersister{dir, fc, indexCache}
}

type fsTablePersister struct {
	dir        string
	fc         *fdCache
	indexCache *indexCache
}

func (ftp *fsTablePersister) Open(name addr, chunkCount uint32, stats *Stats) chunkSource {
	return newMmapTableReader(ftp.dir, name, chunkCount, ftp.indexCache, ftp.fc)
}

func (ftp *fsTablePersister) Persist(mt *memTable, haver chunkReader, stats *Stats) chunkSource {
	name, data, chunkCount := mt.write(haver, stats)
	return ftp.persistTable(name, data, chunkCount, stats)
}

func (ftp *fsTablePersister) persistTable(name addr, data []byte, chunkCount uint32, stats *Stats) chunkSource {
	if chunkCount == 0 {
		return emptyChunkSource{}
	}
	tempName := func() string {
		temp, err := ioutil.TempFile(ftp.dir, tempTablePrefix)
		d.PanicIfError(err)
		defer checkClose(temp)
		io.Copy(temp, bytes.NewReader(data))
		index := parseTableIndex(data)
		if ftp.indexCache != nil {
			ftp.indexCache.lockEntry(name)
			defer ftp.indexCache.unlockEntry(name)
			ftp.indexCache.put(name, index)
		}
		return temp.Name()
	}()
	err := os.Rename(tempName, filepath.Join(ftp.dir, name.String()))
	d.PanicIfError(err)
	return ftp.Open(name, chunkCount, stats)
}

func (ftp *fsTablePersister) ConjoinAll(sources chunkSources, stats *Stats) chunkSource {
	plan := planConjoin(sources, stats)

	if plan.chunkCount == 0 {
		return emptyChunkSource{}
	}

	name := nameFromSuffixes(plan.suffixes())
	tempName := func() string {
		temp, err := ioutil.TempFile(ftp.dir, tempTablePrefix)
		d.PanicIfError(err)
		defer checkClose(temp)

		for _, sws := range plan.sources {
			r := sws.source.reader()
			n, err := io.CopyN(temp, r, int64(sws.dataLen))
			d.PanicIfError(err)
			d.PanicIfFalse(uint64(n) == sws.dataLen)
		}
		_, err = temp.Write(plan.mergedIndex)
		d.PanicIfError(err)

		index := parseTableIndex(plan.mergedIndex)
		if ftp.indexCache != nil {
			ftp.indexCache.put(name, index)
		}
		return temp.Name()
	}()

	err := os.Rename(tempName, filepath.Join(ftp.dir, name.String()))
	d.PanicIfError(err)

	return ftp.Open(name, plan.chunkCount, stats)
}
