// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/sizecache"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	dataAttr    = "data"
	tablePrefix = "*" // I want to use NBS table names as keys when they are written to DynamoDB, but a bare table name is a legal Noms Database name as well. To avoid collisions, dynamoTableReader prepends this prefix (which is not a legal character in a Noms Database name).
)

// dynamoTableReader assumes the existence of a DynamoDB table whose primary partition key is in String format and named `db`.
type dynamoTableReader struct {
	tableReader
	ddb   ddbsvc
	table string
	h     addr
	tc    *sizecache.SizeCache
}

type tableNotInDynamoErr struct {
	nbs, dynamo string
}

func (t tableNotInDynamoErr) Error() string {
	return fmt.Sprintf("NBS table %s not present in DynamoDB table %s", t.nbs, t.dynamo)
}

func newDynamoTableReader(ddb ddbsvc, table string, h addr, chunkCount uint32, data []byte, indexCache *indexCache, tc *sizecache.SizeCache) chunkSource {
	d.PanicIfTrue(table == "")
	source := &dynamoTableReader{ddb: ddb, table: table, h: h, tc: tc}

	var index tableIndex
	found := false
	if indexCache != nil {
		indexCache.lockEntry(h)
		defer indexCache.unlockEntry(h)
		index, found = indexCache.get(h)
	}

	if !found {
		index = parseTableIndex(data)
		if indexCache != nil {
			indexCache.put(h, index)
		}
	}

	source.tableReader = newTableReader(index, source, fileBlockSize)
	d.PanicIfFalse(chunkCount == source.count())
	return source
}

func (dtr *dynamoTableReader) hash() addr {
	return dtr.h
}

func (dtr *dynamoTableReader) ReadAtWithStats(p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()
	if data, present := dynamoTableCacheMaybeGet(dtr.tc, dtr.hash()); present {
		defer func() {
			stats.MemBytesPerRead.Sample(uint64(len(p)))
			stats.MemReadLatency.SampleTimeSince(t1)
		}()
		n = copy(p, data[off:])
		if n < len(p) {
			err = io.ErrUnexpectedEOF
		}
		return
	}

	defer func() {
		stats.DynamoBytesPerRead.Sample(uint64(len(p)))
		stats.DynamoReadLatency.SampleTimeSince(t1)
	}()
	return dtr.readRange(p, off)
}

func (dtr *dynamoTableReader) readRange(p []byte, off int64) (n int, err error) {
	data, err := tryDynamoTableRead(dtr.ddb, dtr.table, dtr.h)
	d.PanicIfError(err)

	dynamoTableCacheMaybeAdd(dtr.tc, dtr.hash(), data)

	n = copy(p, data[off:])
	if n < len(p) {
		err = io.ErrUnexpectedEOF
	}
	return
}

func dynamoTableCacheMaybeGet(tc *sizecache.SizeCache, name addr) (data []byte, present bool) {
	if tc != nil {
		if i, present := tc.Get(name); present {
			return i.([]byte), true
		}
	}
	return
}

func dynamoTableCacheMaybeAdd(tc *sizecache.SizeCache, name addr, data []byte) {
	if tc != nil {
		tc.Add(name, uint64(len(data)), data)
	}
}

func tryDynamoTableRead(ddb ddbsvc, table string, name addr) (data []byte, err error) {
	try := func(input *dynamodb.GetItemInput) (data []byte, err error) {
		result, rerr := ddb.GetItem(input)
		if rerr != nil {
			return nil, rerr
		} else if len(result.Item) == 0 {
			return nil, tableNotInDynamoErr{name.String(), table}
		} else if result.Item[dataAttr] == nil || result.Item[dataAttr].B == nil {
			return nil, fmt.Errorf("NBS table %s in DynamoDB table %s is malformed", name, table)
		}
		return result.Item[dataAttr].B, nil
	}

	input := dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			dbAttr: {S: aws.String(fmtTableName(name))},
		},
	}
	data, err = try(&input)
	if _, isNotFound := err.(tableNotInDynamoErr); isNotFound {
		log.Printf("Eventually consistent read for %s failed; trying fully-consistent", name)
		input.ConsistentRead = aws.Bool(true)
		return try(&input)
	}
	return data, err
}

func fmtTableName(name addr) string {
	return tablePrefix + name.String()
}

func dynamoTableWrite(ddb ddbsvc, table string, name addr, data []byte) error {
	_, err := ddb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:   {S: aws.String(fmtTableName(name))},
			dataAttr: {B: data},
		},
	})
	return err
}
