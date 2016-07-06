// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type mockAWSError string

func (m mockAWSError) Error() string   { return string(m) }
func (m mockAWSError) Code() string    { return string(m) }
func (m mockAWSError) Message() string { return string(m) }
func (m mockAWSError) OrigErr() error  { return nil }

type fakeDDB struct {
	data        map[string]record
	assert      *assert.Assertions
	numPuts     int
	numCompPuts int
	version     string
}

type record struct {
	chunk []byte
	comp  string
}

func createFakeDDB(a *assert.Assertions) *fakeDDB {
	return &fakeDDB{
		data:    map[string]record{},
		assert:  a,
		version: constants.NomsVersion,
	}
}

func (m *fakeDDB) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	m.assert.Len(input.RequestItems, 1)
	out := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]*dynamodb.AttributeValue{}}
	for tableName, keysAndAttrs := range input.RequestItems {
		out.Responses[tableName] = nil
		for _, keyMap := range keysAndAttrs.Keys {
			key := keyMap[refAttr].B
			value, comp := m.get(key)

			if value != nil {
				item := map[string]*dynamodb.AttributeValue{
					refAttr:   {B: key},
					chunkAttr: {B: value},
					compAttr:  {S: aws.String(comp)},
				}
				out.Responses[tableName] = append(out.Responses[tableName], item)
			}
		}
	}
	return out, nil
}

func (m *fakeDDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	m.assert.Len(input.RequestItems, 1)
	out := &dynamodb.BatchWriteItemOutput{}
	for _, writeReqs := range input.RequestItems {
		for _, writeReq := range writeReqs {
			putReq := writeReq.PutRequest
			m.assert.NotNil(putReq)
			key := putReq.Item[refAttr].B
			value := putReq.Item[chunkAttr].B
			comp := putReq.Item[compAttr].S
			m.assert.NotNil(key, "key should have been a blob: %+v", putReq.Item[refAttr])
			m.assert.NotNil(value, "value should have been a blob: %+v", putReq.Item[chunkAttr])
			m.assert.NotNil(comp, "comp should have been a string: %+v", putReq.Item[compAttr])
			m.assert.False(bytes.Equal(key, dynamoRootKey), "Can't batch-write the root!")

			m.put(key, value, *comp)
			if *comp != noneValue {
				m.numCompPuts++
			}
			m.numPuts++
		}
	}
	return out, nil
}

func (m *fakeDDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	key := input.Key[refAttr].B
	m.assert.NotNil(key, "key should have been a blob: %+v", input.Key[refAttr])

	item := map[string]*dynamodb.AttributeValue{}
	if bytes.HasSuffix(key, dynamoVersionKey) {
		item[refAttr] = &dynamodb.AttributeValue{B: key}
		item[numAttr] = &dynamodb.AttributeValue{S: aws.String(m.version)}
	} else {
		value, comp := m.get(key)
		if value != nil {
			item[refAttr] = &dynamodb.AttributeValue{B: key}
			item[chunkAttr] = &dynamodb.AttributeValue{B: value}
			item[compAttr] = &dynamodb.AttributeValue{S: aws.String(comp)}
		}
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) get(k []byte) ([]byte, string) {
	return m.data[string(k)].chunk, m.data[string(k)].comp
}

func (m *fakeDDB) put(k, v []byte, c string) {
	m.data[string(k)] = record{v, c}
}

func (m *fakeDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	m.assert.NotNil(input.Item[refAttr], "%s should have been present", refAttr)
	m.assert.NotNil(input.Item[refAttr].B, "key should have been a blob: %+v", input.Item[refAttr])
	key := input.Item[refAttr].B
	if bytes.HasSuffix(key, dynamoVersionKey) {
		m.assert.NotNil(input.Item[numAttr], "%s should have been present", numAttr)
		m.assert.NotNil(input.Item[numAttr].S, "vers should have been a string: %+v", input.Item[numAttr])
		m.version = aws.StringValue(input.Item[numAttr].S)
		return &dynamodb.PutItemOutput{}, nil
	}
	m.assert.NotNil(input.Item[chunkAttr], "%s should have present", chunkAttr)
	m.assert.NotNil(input.Item[chunkAttr].B, "value should have been a blob: %+v", input.Item[chunkAttr])
	value := input.Item[chunkAttr].B

	mustNotExist := *(input.ConditionExpression) == valueNotExistsExpression
	current, present := m.data[string(key)]

	if mustNotExist && present {
		return nil, mockAWSError("ConditionalCheckFailedException")
	} else if !mustNotExist && !bytes.Equal(current.chunk, input.ExpressionAttributeValues[":prev"].B) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.put(key, value, noneValue)
	if !bytes.HasSuffix(key, dynamoRootKey) {
		m.numPuts++
	}

	return &dynamodb.PutItemOutput{}, nil
}

type lowCapFakeDDB struct {
	fakeDDB
	firstTry bool
}

func createLowCapFakeDDB(a *assert.Assertions) *lowCapFakeDDB {
	return &lowCapFakeDDB{fakeDDB{data: map[string]record{}, assert: a}, true}
}

func (m *lowCapFakeDDB) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	m.assert.Len(input.RequestItems, 1)
	if m.firstTry {
		m.firstTry = false
		return &dynamodb.BatchGetItemOutput{UnprocessedKeys: input.RequestItems}, mockAWSError("ProvisionedThroughputExceededException")
	}

	out := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]*dynamodb.AttributeValue{}}
	for tableName, keysAndAttrs := range input.RequestItems {
		out.Responses[tableName] = nil
		key := keysAndAttrs.Keys[0][refAttr].B

		value, comp := m.get(key)
		if value != nil {
			item := map[string]*dynamodb.AttributeValue{
				refAttr:   {B: key},
				chunkAttr: {B: value},
				compAttr:  {S: aws.String(comp)},
			}
			out.Responses[tableName] = append(out.Responses[tableName], item)
		}
	}
	return out, nil
}
