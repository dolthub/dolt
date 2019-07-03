// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

const tempTablePrefix = "nbs_table_"

func newFSTablePersister(dir string, fc *fdCache, indexCache *indexCache) tablePersister {
	// TODO: fix panics
	d.PanicIfTrue(fc == nil)
	return &fsTablePersister{dir, fc, indexCache}
}

type fsTablePersister struct {
	dir        string
	fc         *fdCache
	indexCache *indexCache
}

func (ftp *fsTablePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	return newMmapTableReader(ftp.dir, name, chunkCount, ftp.indexCache, ftp.fc)
}

func (ftp *fsTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	name, data, chunkCount := mt.write(haver, stats)
	return ftp.persistTable(ctx, name, data, chunkCount, stats)
}

func (ftp *fsTablePersister) persistTable(ctx context.Context, name addr, data []byte, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	tempName, err := func() (string, error) {
		temp, err := ioutil.TempFile(ftp.dir, tempTablePrefix)

		if err != nil {
			return "", err
		}

		defer mustClose(temp)

		_, err = io.Copy(temp, bytes.NewReader(data))

		if err != nil {
			return "", err
		}

		index, err := parseTableIndex(data)

		if err != nil {
			return "", err
		}

		if ftp.indexCache != nil {
			ftp.indexCache.lockEntry(name)
			defer ftp.indexCache.unlockEntry(name)
			ftp.indexCache.put(name, index)
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return nil, err
	}

	newName := filepath.Join(ftp.dir, name.String())
	ftp.fc.ShrinkCache()

	err = os.Rename(tempName, newName)

	if err != nil {
		return nil, err
	}

	return ftp.Open(ctx, name, chunkCount, stats)
}

func (ftp *fsTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	plan := planConjoin(sources, stats)

	if plan.chunkCount == 0 {
		return emptyChunkSource{}, nil
	}

	name := nameFromSuffixes(plan.suffixes())
	tempName, err := func() (string, error) {
		temp, err := ioutil.TempFile(ftp.dir, tempTablePrefix)

		if err != nil {
			return "", err
		}

		defer mustClose(temp)

		for _, sws := range plan.sources {
			r, err := sws.source.reader(ctx)

			if err != nil {
				return "", err
			}

			n, err := io.CopyN(temp, r, int64(sws.dataLen))

			if err != nil {
				return "", err
			}

			if uint64(n) != sws.dataLen {
				return "", errors.New("failed to copy all data")
			}
		}

		_, err = temp.Write(plan.mergedIndex)

		if err != nil {
			return "", err
		}

		index, err := parseTableIndex(plan.mergedIndex)

		if err != nil {
			return "", err
		}

		if ftp.indexCache != nil {
			ftp.indexCache.put(name, index)
		}

		return temp.Name(), nil
	}()

	if err != nil {
		return nil, err
	}

	err = os.Rename(tempName, filepath.Join(ftp.dir, name.String()))

	if err != nil {
		return nil, err
	}

	return ftp.Open(ctx, name, plan.chunkCount, stats)
}
