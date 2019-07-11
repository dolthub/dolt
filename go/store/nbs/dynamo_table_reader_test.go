// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/liquidata-inc/ld/dolt/go/store/util/sizecache"
	"github.com/stretchr/testify/assert"
)

func TestDynamoTableReaderAt(t *testing.T) {
	ddb := makeFakeDDB(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h, err := buildTable(chunks)
	assert.NoError(t, err)
	ddb.putData(fmtTableName(h), tableData)

	t.Run("ddbTableStore", func(t *testing.T) {
		t.Run("ReadTable", func(t *testing.T) {
			test := func(dts *ddbTableStore) {
				assert := assert.New(t)
				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				assert.NoError(err)
				assert.Equal(tableData, data)

				data, err = dts.ReadTable(context.Background(), computeAddr([]byte{}), &Stats{})
				assert.Error(err)
				assert.IsType(tableNotInDynamoErr{}, err)
				assert.Nil(data)
			}

			t.Run("EventuallyConsistentSuccess", func(t *testing.T) {
				test(&ddbTableStore{ddb, "table", nil, nil})
			})

			t.Run("EventuallyConsistentFailure", func(t *testing.T) {
				test(&ddbTableStore{&eventuallyConsistentDDB{ddb}, "table", nil, nil})
			})

			t.Run("WithCache", func(t *testing.T) {
				tc := sizecache.New(uint64(2 * len(tableData)))
				dts := &ddbTableStore{ddb, "table", nil, tc}
				test(dts)

				// Table should have been cached on read
				baseline := ddb.numGets
				_, err := dts.ReadTable(context.Background(), h, &Stats{})
				assert.NoError(t, err)
				assert.Zero(t, ddb.numGets-baseline)
			})
		})

		t.Run("WriteTable", func(t *testing.T) {
			t.Run("WithoutCache", func(t *testing.T) {
				assert := assert.New(t)

				dts := &ddbTableStore{makeFakeDDB(t), "table", nil, nil}
				assert.NoError(dts.Write(context.Background(), h, tableData))

				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				assert.NoError(err)
				assert.Equal(tableData, data)
			})

			t.Run("WithCache", func(t *testing.T) {
				assert := assert.New(t)

				tc := sizecache.New(uint64(2 * len(tableData)))
				dts := &ddbTableStore{makeFakeDDB(t), "table", nil, tc}
				assert.NoError(dts.Write(context.Background(), h, tableData))

				// Table should have been cached on write
				baseline := ddb.numGets
				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				assert.NoError(err)
				assert.Equal(tableData, data)
				assert.Zero(ddb.numGets - baseline)
			})
		})
	})

	t.Run("ReadAtWithCache", func(t *testing.T) {
		assert := assert.New(t)
		stats := &Stats{}

		tc := sizecache.New(uint64(2 * len(tableData)))
		tra := &dynamoTableReaderAt{&ddbTableStore{ddb, "table", nil, tc}, h}

		// First, read when table is not yet cached
		scratch := make([]byte, len(tableData)/4)
		baseline := ddb.numGets
		_, err := tra.ReadAtWithStats(context.Background(), scratch, 0, stats)
		assert.NoError(err)
		assert.True(ddb.numGets > baseline)

		// Table should have been cached on read so read again, a different slice this time
		baseline = ddb.numGets
		_, err = tra.ReadAtWithStats(context.Background(), scratch, int64(len(scratch)), stats)
		assert.NoError(err)
		assert.Zero(ddb.numGets - baseline)
	})
}

type eventuallyConsistentDDB struct {
	ddbsvc
}

func (ec *eventuallyConsistentDDB) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	if input.ConsistentRead != nil && *(input.ConsistentRead) {
		return ec.ddbsvc.GetItemWithContext(ctx, input)
	}
	return &dynamodb.GetItemOutput{}, nil
}
