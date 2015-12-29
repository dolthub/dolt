package chunks

import (
	"bytes"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

type mockAWSError string

func (m mockAWSError) Error() string   { return string(m) }
func (m mockAWSError) Code() string    { return string(m) }
func (m mockAWSError) Message() string { return string(m) }
func (m mockAWSError) OrigErr() error  { return nil }

type fakeDDB struct {
	data    map[string][]byte
	numPuts int
	assert  *assert.Assertions
}

func createFakeDDB(a *assert.Assertions) *fakeDDB {
	return &fakeDDB{map[string][]byte{}, 0, a}
}

func (m *fakeDDB) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	m.assert.Len(input.RequestItems, 1)
	out := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]*dynamodb.AttributeValue{}}
	for tableName, keysAndAttrs := range input.RequestItems {
		out.Responses[tableName] = nil
		for _, keyMap := range keysAndAttrs.Keys {
			key := keyMap[refAttr].B
			value := m.get(key)

			if value != nil {
				item := map[string]*dynamodb.AttributeValue{
					refAttr:   &dynamodb.AttributeValue{B: key},
					chunkAttr: &dynamodb.AttributeValue{B: value},
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
			m.assert.NotNil(key, "key should have been a blob: %+v", putReq.Item[refAttr])
			m.assert.NotNil(value, "value should have been a blob: %+v", putReq.Item[chunkAttr])
			m.assert.False(bytes.Equal(key, dynamoRootKey), "Can't batch-write the root!")

			m.data[ref.FromSlice(key).String()] = value
			m.numPuts++
		}
	}
	return out, nil
}

func (m *fakeDDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	key := input.Key[refAttr].B
	m.assert.NotNil(key, "key should have been a blob: %+v", input.Key[refAttr])
	value := m.get(key)

	item := map[string]*dynamodb.AttributeValue{}
	if value != nil {
		item[refAttr] = &dynamodb.AttributeValue{B: key}
		item[chunkAttr] = &dynamodb.AttributeValue{B: value}
	}
	return &dynamodb.GetItemOutput{
		Item: item,
	}, nil
}

func (m *fakeDDB) get(k []byte) (v []byte) {
	if bytes.Equal(k, dynamoRootKey) {
		v = m.data[string(dynamoRootKey)]
	} else {
		v = m.data[ref.FromSlice(k).String()]
	}
	return
}

func (m *fakeDDB) hasRoot() bool {
	return m.data[string(dynamoRootKey)] != nil
}

func (m *fakeDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	key := input.Item[refAttr].B
	value := input.Item[chunkAttr].B
	m.assert.NotNil(key, "key should have been a blob: %+v", input.Item[refAttr])
	m.assert.NotNil(value, "value should have been a blob: %+v", input.Item[chunkAttr])

	if bytes.Equal(key, dynamoRootKey) {
		initial := *(input.ConditionExpression) == valueNotExistsExpression

		if (initial && m.hasRoot()) || (!initial && !bytes.Equal(m.data[string(dynamoRootKey)], input.ExpressionAttributeValues[":prev"].B)) {
			return nil, mockAWSError("ConditionalCheckFailedException")
		}

		m.data[string(dynamoRootKey)] = value
	} else {
		m.data[ref.FromSlice(key).String()] = value
		m.numPuts++
	}

	return &dynamodb.PutItemOutput{}, nil
}

type lowCapFakeDDB struct {
	fakeDDB
	firstTry bool
}

func createLowCapFakeDDB(a *assert.Assertions) *lowCapFakeDDB {
	return &lowCapFakeDDB{fakeDDB{map[string][]byte{}, 0, a}, true}
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

		value := m.get(key)
		if value != nil {
			item := map[string]*dynamodb.AttributeValue{
				refAttr:   &dynamodb.AttributeValue{B: key},
				chunkAttr: &dynamodb.AttributeValue{B: value},
			}
			out.Responses[tableName] = append(out.Responses[tableName], item)
		}
	}
	return out, nil
}
