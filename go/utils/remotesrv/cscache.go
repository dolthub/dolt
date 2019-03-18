package main

import (
	"github.com/attic-labs/noms/go/nbs"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"path/filepath"
	"sync"
)

const (
	defaultMemTableSize = 128 * 1024 * 1024
)

type DBCache struct {
	mu  *sync.Mutex
	dbs map[string]*nbs.NomsBlockStore

	fs filesys.Filesys

	s3Api       *s3.S3
	dynamoApi   *dynamodb.DynamoDB
	bucket      string
	dynamoTable string
}

func NewLocalCSCache(filesys filesys.Filesys) *DBCache {
	return &DBCache{
		&sync.Mutex{},
		make(map[string]*nbs.NomsBlockStore),
		filesys,
		nil,
		nil,
		"",
		"",
	}
}

func NewAWSCSCache(bucket, dynamoTable string, s3Api *s3.S3, dynamoApi *dynamodb.DynamoDB) *DBCache {
	return &DBCache{
		&sync.Mutex{},
		make(map[string]*nbs.NomsBlockStore),
		nil,
		s3Api,
		dynamoApi,
		bucket,
		dynamoTable,
	}
}

func (cache *DBCache) Get(org, repo string) (*nbs.NomsBlockStore, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	id := filepath.Join(org, repo)

	if cs, ok := cache.dbs[id]; ok {
		return cs, nil
	}

	var newCS *nbs.NomsBlockStore
	if cache.fs != nil {
		err := cache.fs.MkDirs(id)

		if err != nil {
			return nil, err
		}

		newCS = nbs.NewLocalStore(id, defaultMemTableSize)
	} else {
		newCS = nbs.NewAWSStore(cache.dynamoTable, id, cache.bucket, cache.s3Api, cache.dynamoApi, defaultMemTableSize)
	}

	cache.dbs[id] = newCS

	return newCS, nil
}
