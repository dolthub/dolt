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

type AWSStoreFactory struct {
	ddb       ddbsvc
	persister tablePersister
	table     string
	conjoiner conjoiner
}

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
		newAsyncConjoiner(awsMaxTables),
	}
}

func (asf *AWSStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	return newAWSStore(asf.table, ns, asf.ddb, asf.persister, asf.conjoiner, defaultMemTableSize)
}

func (asf *AWSStoreFactory) Shutter() {
}

type LocalStoreFactory struct {
	dir        string
	fc         *fdCache
	indexCache *indexCache
	conjoiner  conjoiner
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
	return &LocalStoreFactory{dir, fc, indexCache, newAsyncConjoiner(defaultMaxTables)}
}

func (lsf *LocalStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	err := os.MkdirAll(path, 0777)
	d.PanicIfError(err)
	return newLocalStore(path, defaultMemTableSize, lsf.fc, lsf.indexCache, lsf.conjoiner)
}

func (lsf *LocalStoreFactory) Shutter() {
	lsf.fc.Drop()
}
