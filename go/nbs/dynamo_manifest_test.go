// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	table = "testTable"
	db    = "testDB"
)

func makeDynamoManifestFake(t *testing.T) (mm manifest, ddb *fakeDDB) {
	ddb = makeFakeDDB(assert.New(t))
	mm = newDynamoManifest(table, db, ddb)
	return
}

func TestDynamoManifestParseIfExists(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)

	exists, vers, root, tableSpecs := mm.ParseIfExists(nil)
	assert.False(exists)

	// Simulate another process writing a manifest (with an old Noms version).
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	ddb.put(db, newRoot[:], "0", tableName.String()+":"+"0")

	// ParseIfExists should now reflect the manifest written above.
	exists, vers, root, tableSpecs = mm.ParseIfExists(nil)
	assert.True(exists)
	assert.Equal("0", vers)
	assert.Equal(newRoot, root)
	if assert.Len(tableSpecs, 1) {
		assert.Equal(tableName.String(), tableSpecs[0].name.String())
		assert.Equal(uint32(0), tableSpecs[0].chunkCount)
	}
}

func TestDynamoManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)

	// Simulate another process having already put old Noms data in dir/.
	badRoot := hash.Of([]byte("bad root"))
	ddb.put(db, badRoot[:], "0", "")

	assert.Panics(func() { mm.Update(nil, badRoot, hash.Hash{}, nil) })
}

func TestDynamoManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	mm, _ := makeDynamoManifestFake(t)

	// First, test winning the race against another process.
	newRoot := hash.Of([]byte("new root"))
	specs := []tableSpec{{computeAddr([]byte("a")), 3}}
	actual, tableSpecs := mm.Update(specs, hash.Hash{}, newRoot, nil)
	assert.Equal(newRoot, actual)
	assert.Equal(specs, tableSpecs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	newRoot2 := hash.Of([]byte("new root 2"))
	actual, tableSpecs = mm.Update(nil, hash.Hash{}, newRoot2, nil)
	assert.Equal(newRoot, actual)
	assert.Equal(specs, tableSpecs)
	actual, tableSpecs = mm.Update(nil, actual, newRoot2, nil)
}

type fakeDDB struct {
	data    map[string]record
	assert  *assert.Assertions
	numPuts int
}

type record struct {
	root        []byte
	vers, specs string
}

func makeFakeDDB(a *assert.Assertions) *fakeDDB {
	return &fakeDDB{
		data:   map[string]record{},
		assert: a,
	}
}

func (m *fakeDDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	key := input.Key[dbAttr].S
	m.assert.NotNil(key, "key should have been a String: %+v", input.Key[dbAttr])

	item := map[string]*dynamodb.AttributeValue{}
	root, vers, specs := m.get(*key)
	if root != nil {
		item[dbAttr] = &dynamodb.AttributeValue{S: key}
		item[nbsVersAttr] = &dynamodb.AttributeValue{S: aws.String(StorageVersion)}
		item[versAttr] = &dynamodb.AttributeValue{S: aws.String(vers)}
		item[rootAttr] = &dynamodb.AttributeValue{B: root}
		item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(specs)}
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) get(k string) ([]byte, string, string) {
	return m.data[k].root, m.data[k].vers, m.data[k].specs
}

func (m *fakeDDB) put(k string, r []byte, v string, s string) {
	m.data[k] = record{r, v, s}
}

func (m *fakeDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	m.assert.NotNil(input.Item[dbAttr], "%s should have been present", dbAttr)
	m.assert.NotNil(input.Item[dbAttr].S, "key should have been a String: %+v", input.Item[dbAttr])
	key := *input.Item[dbAttr].S

	m.assert.NotNil(input.Item[nbsVersAttr], "%s should have been present", nbsVersAttr)
	m.assert.NotNil(input.Item[nbsVersAttr].S, "nbsVers should have been a String: %+v", input.Item[nbsVersAttr])
	m.assert.Equal(StorageVersion, *input.Item[nbsVersAttr].S)

	m.assert.NotNil(input.Item[versAttr], "%s should have been present", versAttr)
	m.assert.NotNil(input.Item[versAttr].S, "nbsVers should have been a String: %+v", input.Item[versAttr])
	m.assert.Equal(constants.NomsVersion, *input.Item[versAttr].S)

	m.assert.NotNil(input.Item[rootAttr], "%s should have present", rootAttr)
	m.assert.NotNil(input.Item[rootAttr].B, "root should have been a blob: %+v", input.Item[rootAttr])
	root := input.Item[rootAttr].B

	m.assert.NotNil(input.Item[tableSpecsAttr], "%s should have been present", tableSpecsAttr)
	m.assert.NotNil(input.Item[tableSpecsAttr].S, "specs should have been a String: %+v", input.Item[tableSpecsAttr])
	specs := *input.Item[tableSpecsAttr].S

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current, present := m.data[key]

	if mustNotExist && present {
		return nil, mockAWSError("ConditionalCheckFailedException")
	} else if !mustNotExist && !checkCondition(current, input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.put(key, root, constants.NomsVersion, specs)
	m.numPuts++

	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current record, expressionAttrVals map[string]*dynamodb.AttributeValue) bool {
	return current.vers == *expressionAttrVals[":vers"].S && bytes.Equal(current.root, expressionAttrVals[":prev"].B)
}
