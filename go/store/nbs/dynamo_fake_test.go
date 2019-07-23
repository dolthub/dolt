// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
)

type fakeDDB struct {
	data             map[string]interface{}
	t                *testing.T
	numPuts, numGets int
}

type record struct {
	lock, root  []byte
	vers, specs string
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
	if e, present := m.data[*key]; present {
		item[dbAttr] = &dynamodb.AttributeValue{S: key}
		switch e := e.(type) {
		case record:
			item[nbsVersAttr] = &dynamodb.AttributeValue{S: aws.String(StorageVersion)}
			item[versAttr] = &dynamodb.AttributeValue{S: aws.String(e.vers)}
			item[rootAttr] = &dynamodb.AttributeValue{B: e.root}
			item[lockAttr] = &dynamodb.AttributeValue{B: e.lock}
			if e.specs != "" {
				item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(e.specs)}
			}
		case []byte:
			item[dataAttr] = &dynamodb.AttributeValue{B: e}
		}
	}
	m.numGets++
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) putRecord(k string, l, r []byte, v string, s string) {
	m.data[k] = record{l, r, v, s}
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
	assert.Equal(m.t, StorageVersion, *input.Item[nbsVersAttr].S)

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
	} else if !mustNotExist && !checkCondition(current.(record), input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.putRecord(key, lock, root, constants.NomsVersion, specs)
	m.numPuts++

	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current record, expressionAttrVals map[string]*dynamodb.AttributeValue) bool {
	return current.vers == *expressionAttrVals[":vers"].S && bytes.Equal(current.lock, expressionAttrVals[":prev"].B)
}
