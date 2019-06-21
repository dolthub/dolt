// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/liquidata-inc/ld/dolt/go/store/go/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	"github.com/liquidata-inc/ld/dolt/go/store/go/util/sizecache"
)

const (
	defaultAWSReadLimit = 1024
	awsMaxTables        = 128

	defaultSmallTableCacheSize = 1 << 28 // 256MB
)

// AWSStoreFactory vends NomsBlockStores built on top of DynamoDB and S3.
type AWSStoreFactory struct {
	ddb       ddbsvc
	persister tablePersister
	table     string
	conjoiner conjoiner

	manifestLocks *manifestLocks
	manifestCache *manifestCache
}

// NewAWSStoreFactory returns a ChunkStore factory that vends NomsBlockStore
// instances that store manifests in the named DynamoDB table, and chunk data
// in the named S3 bucket. All connections to AWS services share |sess|.
func NewAWSStoreFactory(ctx context.Context, sess *session.Session, table, bucket string, maxOpenFiles int, indexCacheSize, tableCacheSize uint64, tableCacheDir string) chunks.Factory {
	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	var tc *fsTableCache
	if tableCacheSize > 0 {
		tc = newFSTableCache(tableCacheDir, tableCacheSize, maxOpenFiles)
	}

	ddb := dynamodb.New(sess)
	readRateLimiter := make(chan struct{}, defaultAWSReadLimit)
	return &AWSStoreFactory{
		ddb: ddb,
		persister: &awsTablePersister{
			s3.New(sess),
			bucket,
			readRateLimiter,
			tc,
			&ddbTableStore{ddb, table, readRateLimiter, sizecache.New(defaultSmallTableCacheSize)},
			awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize, maxDynamoItemSize, maxDynamoChunks},
			indexCache,
			"",
		},
		table:         table,
		conjoiner:     inlineConjoiner{awsMaxTables},
		manifestLocks: newManifestLocks(),
		manifestCache: newManifestCache(defaultManifestCacheSize),
	}
}

func (asf *AWSStoreFactory) CreateStore(ctx context.Context, ns string) chunks.ChunkStore {
	mm := manifestManager{newDynamoManifest(asf.table, ns, asf.ddb), asf.manifestCache, asf.manifestLocks}
	return newNomsBlockStore(ctx, mm, asf.persister, asf.conjoiner, defaultMemTableSize)
}

func (asf *AWSStoreFactory) CreateStoreFromCache(ctx context.Context, ns string) chunks.ChunkStore {
	mm := manifestManager{newDynamoManifest(asf.table, ns, asf.ddb), asf.manifestCache, asf.manifestLocks}

	contents, _, present := asf.manifestCache.Get(mm.Name())
	if present {
		return newNomsBlockStoreWithContents(ctx, mm, contents, asf.persister, asf.conjoiner, defaultMemTableSize)
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

	manifestLocks *manifestLocks
	manifestCache *manifestCache
}

func checkDir(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dir)
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
		conjoiner:     inlineConjoiner{defaultMaxTables},
		manifestLocks: newManifestLocks(),
		manifestCache: newManifestCache(defaultManifestCacheSize),
	}
}

func (lsf *LocalStoreFactory) CreateStore(ctx context.Context, ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	d.PanicIfError(os.MkdirAll(path, 0777))

	mm := manifestManager{fileManifest{path}, lsf.manifestCache, lsf.manifestLocks}
	p := newFSTablePersister(path, lsf.fc, lsf.indexCache)
	return newNomsBlockStore(ctx, mm, p, lsf.conjoiner, defaultMemTableSize)
}

func (lsf *LocalStoreFactory) CreateStoreFromCache(ctx context.Context, ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	mm := manifestManager{fileManifest{path}, lsf.manifestCache, lsf.manifestLocks}

	contents, _, present := lsf.manifestCache.Get(mm.Name())
	if present {
		_, err := os.Stat(path)
		d.PanicIfTrue(os.IsNotExist(err))
		p := newFSTablePersister(path, lsf.fc, lsf.indexCache)
		return newNomsBlockStoreWithContents(ctx, mm, contents, p, lsf.conjoiner, defaultMemTableSize)
	}
	return nil
}

func (lsf *LocalStoreFactory) Shutter() {
	lsf.fc.Drop()
}
