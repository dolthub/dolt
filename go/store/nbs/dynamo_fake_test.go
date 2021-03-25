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

type appendix struct {
	lock, root  []byte
	vers, specs string
}

type record struct {
	lock, root  []byte
	vers, specs string
	appendix
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
		ti, err := parseTableIndex(buff)

		if err != nil {
			return nil, err
		}

		return newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize), nil
	}
	return nil, nil
}

func (m *fakeDDB) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	key := input.Key[dbAttr].S
	assert.NotNil(m.t, key, "key should have been a String: %+v", input.Key[dbAttr])

	item := map[string]*dynamodb.AttributeValue{}
	app := map[string]*dynamodb.AttributeValue{}
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
			app[versAttr] = &dynamodb.AttributeValue{S: aws.String(e.appendix.vers)}
			app[rootAttr] = &dynamodb.AttributeValue{B: e.appendix.root}
			app[lockAttr] = &dynamodb.AttributeValue{B: e.appendix.lock}
			if e.appendix.specs != "" {
				app[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(e.appendix.specs)}
			}
			item[appendixAttr] = &dynamodb.AttributeValue{M: app}
		case []byte:
			item[dataAttr] = &dynamodb.AttributeValue{B: e}
		}
	}
	atomic.AddInt64(&m.numGets, 1)
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) hasRecord(k string) bool {
	_, found := m.data[k]
	return found
}

func (m *fakeDDB) getRecordAppendix(k string) appendix {
	r, found := m.data[k]
	if found {
		rSt, rStOk := r.(record)
		if rStOk {
			return rSt.appendix
		}
	}
	return appendix{}
}

func (m *fakeDDB) putRecord(k string, l, r []byte, v string, s string) {
	m.data[k] = record{l, r, v, s, m.getRecordAppendix(k)}
}

func (m *fakeDDB) updateRecord(k string, a appendix) {
	r, ok := m.data[k]
	if ok {
		rSt, rStOk := r.(record)
		if rStOk {
			rSt.appendix = a
			m.data[k] = rSt
		}
	}
}

func (m *fakeDDB) putData(k string, d []byte) {
	m.data[k] = d
}

func (m *fakeDDB) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	assert.NotNil(m.t, input.Key[dbAttr], "%s should have been present", dbAttr)
	assert.NotNil(m.t, input.Key[dbAttr].S, "key should have been a String: %+v", input.Key[dbAttr])

	key := *input.Key[dbAttr].S
	assert.True(m.t, m.hasRecord(key))

	assert.NotNil(m.t, input.ExpressionAttributeNames, "ExpressionAttributesNames should have been present")
	assert.NotNil(m.t, input.ExpressionAttributeNames[updateExpressionNamesKey], "%s should have been present", updateExpressionNamesKey)
	assert.Equal(m.t, *input.ExpressionAttributeNames[updateExpressionNamesKey], appendixAttr)

	assert.NotNil(m.t, input.ExpressionAttributeValues, "ExpressionAttributesValues should have been present")
	assert.NotNil(m.t, input.ExpressionAttributeValues[updateExpressionValuesKey], "%s should have been present", updateExpressionValuesKey)
	assert.NotNil(m.t, input.ExpressionAttributeValues[updateExpressionValuesKey].M, "value should have been a Map: %+v", input.ExpressionAttributeValues[updateExpressionValuesKey])
	assert.NotNil(m.t, input.UpdateExpression, "%s should have been present", updateExpressionValuesKey)

	app := appendix{}
	appMap := input.ExpressionAttributeValues[updateExpressionValuesKey].M

	assert.NotNil(m.t, appMap[versAttr], "%s should have been present", versAttr)
	assert.NotNil(m.t, appMap[versAttr].S, "nbsVers should have been a String: %+v", appMap[versAttr])
	assert.Equal(m.t, constants.NomsVersion, *appMap[versAttr].S)
	app.vers = *appMap[versAttr].S

	assert.NotNil(m.t, appMap[lockAttr], "%s should have been present", lockAttr)
	assert.NotNil(m.t, appMap[lockAttr].B, "lock should have been a blob: %+v", appMap[lockAttr])
	app.lock = appMap[lockAttr].B

	assert.NotNil(m.t, appMap[rootAttr], "%s should have been present", rootAttr)
	assert.NotNil(m.t, appMap[rootAttr].B, "root should have been a blob: %+v", appMap[rootAttr])
	app.root = appMap[rootAttr].B

	if specsAttr, ok := appMap[tableSpecsAttr]; ok {
		assert.NotNil(m.t, specsAttr.S, "specs should have been a String: %+v", appMap[tableSpecsAttr])
		app.specs = *specsAttr.S
	}

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current := m.getRecordAppendix(key)

	if mustNotExist {
		if current.lock != nil || len(current.lock) > 0 ||
			current.root != nil || len(current.root) > 0 ||
			current.vers != "" {
			return nil, mockAWSError("ConditionalCheckFailedException")
		}
	}

	if !mustNotExist && !checkCondition(current, input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.updateRecord(key, app)

	atomic.AddInt64(&m.numPuts, 1)
	return &dynamodb.UpdateItemOutput{}, nil
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

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current, present := m.data[key]

	if mustNotExist && present {
		return nil, mockAWSError("ConditionalCheckFailedException")
	} else if !mustNotExist && !checkCondition(current, input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.putRecord(key, lock, root, constants.NomsVersion, specs)

	atomic.AddInt64(&m.numPuts, 1)
	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current interface{}, expressionAttrVals map[string]*dynamodb.AttributeValue) bool {
	switch c := current.(type) {
	case record:
		return c.vers == *expressionAttrVals[versExpressionValuesKey].S && bytes.Equal(c.lock, expressionAttrVals[prevLockExpressionValuesKey].B)
	case appendix:
		return c.vers == *expressionAttrVals[versExpressionValuesKey].S && bytes.Equal(c.lock, expressionAttrVals[prevLockExpressionValuesKey].B)
	}
	return false
}

func (m *fakeDDB) NumGets() int64 {
	return atomic.LoadInt64(&m.numGets)
}
