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
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/util/sizecache"
)

func TestDynamoTableReaderAt(t *testing.T) {
	ddb := makeFakeDDB(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h, err := buildTable(chunks)
	require.NoError(t, err)
	ddb.putData(fmtTableName(h), tableData)

	t.Run("ddbTableStore", func(t *testing.T) {
		t.Run("ReadTable", func(t *testing.T) {
			test := func(dts *ddbTableStore) {
				assert := assert.New(t)
				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				require.NoError(t, err)
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
				baseline := ddb.NumGets()
				_, err := dts.ReadTable(context.Background(), h, &Stats{})
				require.NoError(t, err)
				assert.Zero(t, ddb.NumGets()-baseline)
			})
		})

		t.Run("WriteTable", func(t *testing.T) {
			t.Run("WithoutCache", func(t *testing.T) {
				assert := assert.New(t)

				dts := &ddbTableStore{makeFakeDDB(t), "table", nil, nil}
				require.NoError(t, dts.Write(context.Background(), h, tableData))

				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				require.NoError(t, err)
				assert.Equal(tableData, data)
			})

			t.Run("WithCache", func(t *testing.T) {
				assert := assert.New(t)

				tc := sizecache.New(uint64(2 * len(tableData)))
				dts := &ddbTableStore{makeFakeDDB(t), "table", nil, tc}
				require.NoError(t, dts.Write(context.Background(), h, tableData))

				// Table should have been cached on write
				baseline := ddb.NumGets()
				data, err := dts.ReadTable(context.Background(), h, &Stats{})
				require.NoError(t, err)
				assert.Equal(tableData, data)
				assert.Zero(ddb.NumGets() - baseline)
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
		baseline := ddb.NumGets()
		_, err := tra.ReadAtWithStats(context.Background(), scratch, 0, stats)
		require.NoError(t, err)
		assert.True(ddb.NumGets() > baseline)

		// Table should have been cached on read so read again, a different slice this time
		baseline = ddb.NumGets()
		_, err = tra.ReadAtWithStats(context.Background(), scratch, int64(len(scratch)), stats)
		require.NoError(t, err)
		assert.Zero(ddb.NumGets() - baseline)
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
