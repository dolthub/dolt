// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/dolthub/dolt/go/store/util/sizecache"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

const (
	dataAttr    = "data"
	tablePrefix = "*" // I want to use NBS table names as keys when they are written to DynamoDB, but a bare table name is a legal Noms Database name as well. To avoid collisions, dynamoTableReader prepends this prefix (which is not a legal character in a Noms Database name).
)

// dynamoTableReaderAt assumes the existence of a DynamoDB table whose primary partition key is in String format and named `db`.
type dynamoTableReaderAt struct {
	ddb *ddbTableStore
	h   addr
}

type tableNotInDynamoErr struct {
	nbs, dynamo string
}

func (t tableNotInDynamoErr) Error() string {
	return fmt.Sprintf("NBS table %s not present in DynamoDB table %s", t.nbs, t.dynamo)
}

func (dtra *dynamoTableReaderAt) Reader(ctx context.Context) (io.ReadCloser, error) {
	data, err := dtra.ddb.ReadTable(ctx, dtra.h, &Stats{})
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (dtra *dynamoTableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	data, err := dtra.ddb.ReadTable(ctx, dtra.h, stats)

	if err != nil {
		return 0, err
	}

	n = copy(p, data[off:])
	if n < len(p) {
		err = io.ErrUnexpectedEOF
	}
	return
}

type ddbTableStore struct {
	ddb    ddbsvc
	table  string
	readRl chan struct{}
	cache  *sizecache.SizeCache // TODO: merge this with tableCache as part of BUG 3601
}

func (dts *ddbTableStore) ReadTable(ctx context.Context, name addr, stats *Stats) (data []byte, err error) {
	t1 := time.Now()
	if dts.cache != nil {
		if i, present := dts.cache.Get(name); present {
			data = i.([]byte)
			defer func() {
				stats.MemBytesPerRead.Sample(uint64(len(data)))
				stats.MemReadLatency.SampleTimeSince(t1)
			}()
			return data, nil
		}
	}

	data, err = dts.readTable(ctx, name)
	if data != nil {
		defer func() {
			stats.DynamoBytesPerRead.Sample(uint64(len(data)))
			stats.DynamoReadLatency.SampleTimeSince(t1)
		}()
	}

	if dts.cache != nil && err == nil {
		dts.cache.Add(name, uint64(len(data)), data)
	}
	return data, err
}

func (dts *ddbTableStore) readTable(ctx context.Context, name addr) (data []byte, err error) {
	try := func(input *dynamodb.GetItemInput) (data []byte, err error) {
		if dts.readRl != nil {
			dts.readRl <- struct{}{}
			defer func() {
				<-dts.readRl
			}()
		}
		result, rerr := dts.ddb.GetItemWithContext(ctx, input)
		if rerr != nil {
			return nil, rerr
		} else if len(result.Item) == 0 {
			return nil, tableNotInDynamoErr{name.String(), dts.table}
		} else if result.Item[dataAttr] == nil || result.Item[dataAttr].B == nil {
			return nil, fmt.Errorf("NBS table %s in DynamoDB table %s is malformed", name, dts.table)
		}
		return result.Item[dataAttr].B, nil
	}

	input := dynamodb.GetItemInput{
		TableName: aws.String(dts.table),
		Key: map[string]*dynamodb.AttributeValue{
			dbAttr: {S: aws.String(fmtTableName(name))},
		},
	}
	data, err = try(&input)
	if _, isNotFound := err.(tableNotInDynamoErr); isNotFound {
		verbose.Logger(ctx).Sugar().Debugf("Eventually consistent read for %s failed; trying fully-consistent", name)
		input.ConsistentRead = aws.Bool(true)
		return try(&input)
	}
	return data, err
}

func fmtTableName(name addr) string {
	return tablePrefix + name.String()
}

func (dts *ddbTableStore) Write(ctx context.Context, name addr, data []byte) error {
	_, err := dts.ddb.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(dts.table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:   {S: aws.String(fmtTableName(name))},
			dataAttr: {B: data},
		},
	})

	if dts.cache != nil && err == nil {
		dts.cache.Add(name, uint64(len(data)), data)
	}
	return err
}
