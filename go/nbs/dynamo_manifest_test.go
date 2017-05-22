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
	stats := &Stats{}

	exists, vers, lock, root, tableSpecs := mm.ParseIfExists(stats, nil)
	assert.False(exists)

	// Simulate another process writing a manifest (with an old Noms version).
	newLock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	ddb.put(db, newLock[:], newRoot[:], "0", tableName.String()+":"+"0")

	// ParseIfExists should now reflect the manifest written above.
	exists, vers, lock, root, tableSpecs = mm.ParseIfExists(stats, nil)
	assert.True(exists)
	assert.Equal("0", vers)
	assert.Equal(newLock, lock)
	assert.Equal(newRoot, root)
	if assert.Len(tableSpecs, 1) {
		assert.Equal(tableName.String(), tableSpecs[0].name.String())
		assert.Equal(uint32(0), tableSpecs[0].chunkCount)
	}
}

func TestDynamoManifestUpdateWontClobberOldVersion(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// Simulate another process having already put old Noms data in dir/.
	lock := computeAddr([]byte("locker"))
	badRoot := hash.Of([]byte("bad root"))
	ddb.put(db, lock[:], badRoot[:], "0", "")

	assert.Panics(func() { mm.Update(lock, addr{}, nil, hash.Hash{}, stats, nil) })
}

func TestDynamoManifestUpdate(t *testing.T) {
	assert := assert.New(t)
	mm, ddb := makeDynamoManifestFake(t)
	stats := &Stats{}

	// First, test winning the race against another process.
	newLock, newRoot := computeAddr([]byte("locker")), hash.Of([]byte("new root"))
	specs := []tableSpec{{computeAddr([]byte("a")), 3}}
	lock, actual, tableSpecs := mm.Update(addr{}, newLock, specs, newRoot, stats, func() {
		// This should fail to get the lock, and therefore _not_ clobber the manifest. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		ddb.put(db, lock[:], newRoot2[:], constants.NomsVersion, "")
	})
	assert.Equal(newLock, lock)
	assert.Equal(newRoot, actual)
	assert.Equal(specs, tableSpecs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	newLock2, newRoot2 := computeAddr([]byte("locker 2")), hash.Of([]byte("new root 2"))
	lock, actual, tableSpecs = mm.Update(addr{}, newLock2, nil, newRoot2, stats, nil)
	assert.Equal(newLock, lock)
	assert.Equal(newRoot, actual)
	assert.Equal(specs, tableSpecs)
	lock, actual, tableSpecs = mm.Update(lock, newLock2, nil, newRoot2, stats, nil)
	assert.Equal(newLock2, lock)
	assert.Equal(newRoot2, actual)
	assert.Empty(tableSpecs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	ddb.put(db, jerkLock[:], newRoot2[:], constants.NomsVersion, tableName.String()+":1")

	newLock3, newRoot3 := computeAddr([]byte("locker 3")), hash.Of([]byte("new root 3"))
	lock, actual, tableSpecs = mm.Update(lock, newLock3, nil, newRoot3, stats, nil)
	assert.Equal(jerkLock, lock)
	assert.Equal(newRoot2, actual)
	assert.Equal([]tableSpec{{tableName, 1}}, tableSpecs)
}

func TestDynamoManifestUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	mm, _ := makeDynamoManifestFake(t)
	stats := &Stats{}

	l := computeAddr([]byte{0x01})
	lock, actual, tableSpecs := mm.Update(addr{}, l, nil, hash.Hash{}, stats, nil)
	assert.Equal(l, lock)
	assert.True(actual.IsEmpty())
	assert.Empty(tableSpecs)
}

type fakeDDB struct {
	data    map[string]record
	assert  *assert.Assertions
	numPuts int
}

type record struct {
	lock, root  []byte
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
	lock, root, vers, specs := m.get(*key)
	if lock != nil {
		item[dbAttr] = &dynamodb.AttributeValue{S: key}
		item[nbsVersAttr] = &dynamodb.AttributeValue{S: aws.String(StorageVersion)}
		item[versAttr] = &dynamodb.AttributeValue{S: aws.String(vers)}
		item[rootAttr] = &dynamodb.AttributeValue{B: root}
		item[lockAttr] = &dynamodb.AttributeValue{B: lock}
		if specs != "" {
			item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(specs)}
		}
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) get(k string) ([]byte, []byte, string, string) {
	return m.data[k].lock, m.data[k].root, m.data[k].vers, m.data[k].specs
}

func (m *fakeDDB) put(k string, l, r []byte, v string, s string) {
	m.data[k] = record{l, r, v, s}
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

	m.assert.NotNil(input.Item[lockAttr], "%s should have been present", lockAttr)
	m.assert.NotNil(input.Item[lockAttr].B, "lock should have been a blob: %+v", input.Item[lockAttr])
	lock := input.Item[lockAttr].B

	m.assert.NotNil(input.Item[rootAttr], "%s should havebeen  present", rootAttr)
	m.assert.NotNil(input.Item[rootAttr].B, "root should have been a blob: %+v", input.Item[rootAttr])
	root := input.Item[rootAttr].B

	specs := ""
	if attr, present := input.Item[tableSpecsAttr]; present {
		m.assert.NotNil(attr.S, "specs should have been a String: %+v", input.Item[tableSpecsAttr])
		specs = *attr.S
	}

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current, present := m.data[key]

	if mustNotExist && present {
		return nil, mockAWSError("ConditionalCheckFailedException")
	} else if !mustNotExist && !checkCondition(current, input.ExpressionAttributeValues) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	m.put(key, lock, root, constants.NomsVersion, specs)
	m.numPuts++

	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current record, expressionAttrVals map[string]*dynamodb.AttributeValue) bool {
	return current.vers == *expressionAttrVals[":vers"].S && bytes.Equal(current.lock, expressionAttrVals[":prev"].B)
}
