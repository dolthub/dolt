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
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/constants"
)

type fakeDDB struct {
	data             map[string]interface{}
	t                *testing.T
	numPuts, numGets int64
}

type record struct {
	lock, root            []byte
	vers, specs, appendix string
}

func makeFakeDDB(t *testing.T) *fakeDDB {
	return &fakeDDB{
		data: map[string]interface{}{},
		t:    t,
	}
}

func (m *fakeDDB) readerForTable(name addr) (chunkReader, error) {
	if i, present := m.data[fmtTableName(name)]; present {
		buff, ok := i.([]byte)
		assert.True(m.t, ok)
		ti, err := parseTableIndex(buff, &noopQuotaProvider{})

		if err != nil {
			return nil, err
		}

		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		if err != nil {
			return nil, err
		}

		return tr, nil
	}
	return nil, nil
}

func (m *fakeDDB) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	key := input.Key[dbAttr].S
	assert.NotNil(m.t, key, "key should have been a String: %+v", input.Key[dbAttr])

	item := map[string]*dynamodb.AttributeValue{}
	if e, present := m.data[*key]; present {
		item[dbAttr] = &dynamodb.AttributeValue{S: key}
		switch e := e.(type) {
		case record:
			item[nbsVersAttr] = &dynamodb.AttributeValue{S: aws.String(AWSStorageVersion)}
			item[versAttr] = &dynamodb.AttributeValue{S: aws.String(e.vers)}
			item[rootAttr] = &dynamodb.AttributeValue{B: e.root}
			item[lockAttr] = &dynamodb.AttributeValue{B: e.lock}
			if e.specs != "" {
				item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(e.specs)}
			}
			if e.appendix != "" {
				item[appendixAttr] = &dynamodb.AttributeValue{S: aws.String(e.appendix)}
			}
		case []byte:
			item[dataAttr] = &dynamodb.AttributeValue{B: e}
		}
	}
	atomic.AddInt64(&m.numGets, 1)
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) putRecord(k string, l, r []byte, v string, s string, a string) {
	m.data[k] = record{l, r, v, s, a}
}

func (m *fakeDDB) putData(k string, d []byte) {
	m.data[k] = d
}

func (m *fakeDDB) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	assert.NotNil(m.t, input.Item[dbAttr], "%s should have been present", dbAttr)
	assert.NotNil(m.t, input.Item[dbAttr].S, "key should have been a String: %+v", input.Item[dbAttr])
	key := *input.Item[dbAttr].S

	if input.Item[dataAttr] != nil {
		assert.NotNil(m.t, input.Item[dataAttr].B, "data should have been a blob: %+v", input.Item[dataAttr])
		m.putData(key, input.Item[dataAttr].B)
		return &dynamodb.PutItemOutput{}, nil
	}

	assert.NotNil(m.t, input.Item[nbsVersAttr], "%s should have been present", nbsVersAttr)
	assert.NotNil(m.t, input.Item[nbsVersAttr].S, "nbsVers should have been a String: %+v", input.Item[nbsVersAttr])
	assert.Equal(m.t, AWSStorageVersion, *input.Item[nbsVersAttr].S)

	assert.NotNil(m.t, input.Item[versAttr], "%s should have been present", versAttr)
	assert.NotNil(m.t, input.Item[versAttr].S, "nbsVers should have been a String: %+v", input.Item[versAttr])
	assert.Equal(m.t, constants.NomsVersion, *input.Item[versAttr].S)

	assert.NotNil(m.t, input.Item[lockAttr], "%s should have been present", lockAttr)
	assert.NotNil(m.t, input.Item[lockAttr].B, "lock should have been a blob: %+v", input.Item[lockAttr])
	lock := input.Item[lockAttr].B

	assert.NotNil(m.t, input.Item[rootAttr], "%s should have been present", rootAttr)
	assert.NotNil(m.t, input.Item[rootAttr].B, "root should have been a blob: %+v", input.Item[rootAttr])
	root := input.Item[rootAttr].B

	specs := ""
	if attr, present := input.Item[tableSpecsAttr]; present {
		assert.NotNil(m.t, attr.S, "specs should have been a String: %+v", input.Item[tableSpecsAttr])
		specs = *attr.S
	}

	apps := ""
	if attr, present := input.Item[appendixAttr]; present {
		assert.NotNil(m.t, attr.S, "appendix specs should have been a String: %+v", input.Item[appendixAttr])
		apps = *attr.S
	}

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current, present := m.data[key]

	if mustNotExist && present {
		return nil, mockAWSError("ConditionalCheckFailedException")
	} else if !mustNotExist && !checkCondition(current.(record), input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.putRecord(key, lock, root, constants.NomsVersion, specs, apps)

	atomic.AddInt64(&m.numPuts, 1)
	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current record, expressionAttrVals map[string]*dynamodb.AttributeValue) bool {
	return current.vers == *expressionAttrVals[versExpressionValuesKey].S && bytes.Equal(current.lock, expressionAttrVals[prevLockExpressionValuesKey].B)

}

func (m *fakeDDB) NumGets() int64 {
	return atomic.LoadInt64(&m.numGets)
}
