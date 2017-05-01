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

type fsTablePersister struct {
	dir        string
	indexCache *indexCache
}

func (ftp fsTablePersister) Open(name addr, chunkCount uint32) chunkSource {
	return newMmapTableReader(ftp.dir, name, chunkCount, ftp.indexCache)
}

func (ftp fsTablePersister) Persist(mt *memTable, haver chunkReader) chunkSource {
	return ftp.persistTable(mt.write(haver))
}

func (ftp fsTablePersister) persistTable(name addr, data []byte, chunkCount uint32) chunkSource {
	if chunkCount == 0 {
		return emptyChunkSource{}
	}
	tempName := func() string {
		temp, err := ioutil.TempFile(ftp.dir, "nbs_table_")
		d.PanicIfError(err)
		defer checkClose(temp)
		io.Copy(temp, bytes.NewReader(data))
		index := parseTableIndex(data)
		if ftp.indexCache != nil {
			ftp.indexCache.put(name, index)
		}
		return temp.Name()
	}()
	err := os.Rename(tempName, filepath.Join(ftp.dir, name.String()))
	d.PanicIfError(err)
	return ftp.Open(name, chunkCount)
}

func (ftp fsTablePersister) CompactAll(sources chunkSources) chunkSource {
	plan := planCompaction(sources)
	if plan.chunkCount == 0 {
		return emptyChunkSource{}
	}

	name := nameFromSuffixes(plan.suffixes())
	tempName := func() string {
		temp, err := ioutil.TempFile(ftp.dir, "nbs_table_")
		d.PanicIfError(err)
		defer checkClose(temp)

		for _, sws := range plan.sources {
			r := sws.source.reader()
			n, err := io.CopyN(temp, r, int64(sws.dataLen))
			d.PanicIfFalse(uint64(n) == sws.dataLen)
			d.PanicIfError(err)
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
	return ftp.Open(name, plan.chunkCount)
}
