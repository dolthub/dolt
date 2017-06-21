// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	defaultAWSReadLimit = 1024
	awsMaxTables        = 128
)

// AWSStoreFactory vends NomsBlockStores built on top of DynamoDB and S3.
type AWSStoreFactory struct {
	ddb       ddbsvc
	persister tablePersister
	table     string
	conjoiner conjoiner

	manifestCacheMu sync.Mutex
	manifestCache   *manifestCache
}

// NewAWSStoreFactory returns a ChunkStore factory that vends NomsBlockStore
// instances that store manifests in the named DynamoDB table, and chunk data
// in the named S3 bucket. All connections to AWS services share |sess|.
func NewAWSStoreFactory(sess *session.Session, table, bucket string, maxOpenFiles int, indexCacheSize, tableCacheSize uint64, tableCacheDir string) chunks.Factory {
	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	var tc *fsTableCache
	if tableCacheSize > 0 {
		tc = newFSTableCache(tableCacheDir, tableCacheSize, maxOpenFiles)
	}

	return &AWSStoreFactory{
		ddb: dynamodb.New(sess),
		persister: &s3TablePersister{
			s3.New(sess),
			bucket,
			defaultS3PartSize,
			minS3PartSize,
			maxS3PartSize,
			indexCache,
			make(chan struct{}, defaultAWSReadLimit),
			tc,
		},
		table:         table,
		conjoiner:     newAsyncConjoiner(awsMaxTables),
		manifestCache: newManifestCache(defaultManifestCacheSize),
	}
}

func (asf *AWSStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	mm := cachingManifest{newDynamoManifest(asf.table, ns, asf.ddb), &asf.manifestCacheMu, asf.manifestCache}
	return newNomsBlockStore(mm, asf.persister, asf.conjoiner, defaultMemTableSize)
}

func (asf *AWSStoreFactory) CreateStoreFromCache(ns string) chunks.ChunkStore {
	mm := cachingManifest{newDynamoManifest(asf.table, ns, asf.ddb), &asf.manifestCacheMu, asf.manifestCache}

	contents, present := func() (manifestContents, bool) {
		asf.manifestCacheMu.Lock()
		defer asf.manifestCacheMu.Unlock()
		return asf.manifestCache.Get(mm.Name())
	}()
	if present {
		return newNomsBlockStoreWithContents(mm, contents, asf.persister, asf.conjoiner, defaultMemTableSize)
	}
	return nil
}

func (asf *AWSStoreFactory) Shutter() {
}

type LocalStoreFactory struct {
	dir        string
	fc         *fdCache
	indexCache *indexCache
	conjoiner  conjoiner

	manifestCacheMu sync.Mutex
	manifestCache   *manifestCache
}

func checkDir(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("Path is not a directory: %s", dir)
	}
	return nil
}

func NewLocalStoreFactory(dir string, indexCacheSize uint64, maxOpenFiles int) chunks.Factory {
	err := checkDir(dir)
	d.PanicIfError(err)

	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	return &LocalStoreFactory{
		dir:           dir,
		fc:            newFDCache(maxOpenFiles),
		indexCache:    indexCache,
		manifestCache: newManifestCache(defaultManifestCacheSize),
		conjoiner:     newAsyncConjoiner(defaultMaxTables),
	}
}

func (lsf *LocalStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	d.PanicIfError(os.MkdirAll(path, 0777))

	mm := cachingManifest{fileManifest{path}, &lsf.manifestCacheMu, lsf.manifestCache}
	p := newFSTablePersister(path, lsf.fc, lsf.indexCache)
	return newNomsBlockStore(mm, p, lsf.conjoiner, defaultMemTableSize)
}

func (lsf *LocalStoreFactory) CreateStoreFromCache(ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	mm := cachingManifest{fileManifest{path}, &lsf.manifestCacheMu, lsf.manifestCache}

	contents, present := func() (manifestContents, bool) {
		lsf.manifestCacheMu.Lock()
		defer lsf.manifestCacheMu.Unlock()
		return lsf.manifestCache.Get(mm.Name())
	}()
	if present {
		_, err := os.Stat(path)
		d.PanicIfTrue(os.IsNotExist(err))
		p := newFSTablePersister(path, lsf.fc, lsf.indexCache)
		return newNomsBlockStoreWithContents(mm, contents, p, lsf.conjoiner, defaultMemTableSize)
	}
	return nil
}

func (lsf *LocalStoreFactory) Shutter() {
	lsf.fc.Drop()
}
