// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"os"
	"path"

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
	ddb           ddbsvc
	persister     tablePersister
	table         string
	manifestCache *manifestCache
	conjoiner     conjoiner
}

// NewAWSStoreFactory returns a ChunkStore factory that vends NomsBlockStore
// instances that store manifests in the named DynamoDB table, and chunk data
// in the named S3 bucket. All connections to AWS services share |sess|.
func NewAWSStoreFactory(sess *session.Session, table, bucket string, indexCacheSize uint64) chunks.Factory {
	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	return &AWSStoreFactory{
		dynamodb.New(sess),
		&s3TablePersister{
			s3.New(sess),
			bucket,
			defaultS3PartSize,
			minS3PartSize,
			maxS3PartSize,
			indexCache,
			make(chan struct{}, defaultAWSReadLimit),
		},
		table,
		newManifestCache(defaultManifestCacheSize),
		newAsyncConjoiner(awsMaxTables),
	}
}

func (asf *AWSStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	return newAWSStore(asf.table, ns, asf.ddb, asf.manifestCache, asf.persister, asf.conjoiner, defaultMemTableSize)
}

func (asf *AWSStoreFactory) Shutter() {
}

type LocalStoreFactory struct {
	dir           string
	fc            *fdCache
	indexCache    *indexCache
	manifestCache *manifestCache
	conjoiner     conjoiner
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
	fc := newFDCache(maxOpenFiles)
	mc := newManifestCache(defaultManifestCacheSize)
	return &LocalStoreFactory{dir, fc, indexCache, mc, newAsyncConjoiner(defaultMaxTables)}
}

func (lsf *LocalStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	d.PanicIfError(os.MkdirAll(path, 0777))

	mm := newFileManifest(path, lsf.manifestCache)
	p := newFSTablePersister(path, lsf.fc, lsf.indexCache)
	return newNomsBlockStore(mm, p, lsf.conjoiner, defaultMemTableSize)
}

func (lsf *LocalStoreFactory) Shutter() {
	lsf.fc.Drop()
}
