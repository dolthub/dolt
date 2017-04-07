// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"strings"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	tableName      = "attic-nbs"
	dbAttr         = "db"
	lockAttr       = "lock"
	rootAttr       = "root"
	versAttr       = "vers"
	nbsVersAttr    = "nbsVers"
	tableSpecsAttr = "specs"
)

var (
	valueEqualsExpression            = fmt.Sprintf("(%s = :prev) and (%s = :vers)", lockAttr, versAttr)
	valueNotExistsOrEqualsExpression = fmt.Sprintf("attribute_not_exists("+lockAttr+") or %s", valueEqualsExpression)
)

type ddbsvc interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// It assumes the existence of a DynamoDB table whose primary partition key is in String format and named `db`.
type dynamoManifest struct {
	table, db string
	ddbsvc    ddbsvc
}

func newDynamoManifest(table, namespace string, ddb ddbsvc) manifest {
	return dynamoManifest{table: table, db: namespace, ddbsvc: ddb}
}

func (dm dynamoManifest) ParseIfExists(readHook func()) (exists bool, vers string, lock addr, root hash.Hash, tableSpecs []tableSpec) {
	result, err := dm.ddbsvc.GetItem(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true), // This doubles the cost :-(
		TableName:      aws.String(dm.table),
		Key: map[string]*dynamodb.AttributeValue{
			dbAttr: {S: aws.String(dm.db)},
		},
	})
	d.PanicIfError(err)

	// !exists(dbAttr) => unitialized store
	if len(result.Item) > 0 {
		valid, hasSpecs := validateManifest(result.Item)
		if !valid {
			d.Panic("Malformed manifest for %s: %+v", dm.db, result.Item)
		}
		exists = true
		vers = *result.Item[versAttr].S
		root = hash.New(result.Item[rootAttr].B)
		copy(lock[:], result.Item[lockAttr].B)
		if hasSpecs {
			tableSpecs = parseSpecs(strings.Split(*result.Item[tableSpecsAttr].S, ":"))
		}
	}

	return
}

func validateManifest(item map[string]*dynamodb.AttributeValue) (valid, hasSpecs bool) {
	if item[nbsVersAttr] != nil && item[nbsVersAttr].S != nil &&
		StorageVersion == *item[nbsVersAttr].S &&
		item[versAttr] != nil && item[versAttr].S != nil &&
		item[lockAttr] != nil && item[lockAttr].B != nil &&
		item[rootAttr] != nil && item[rootAttr].B != nil {
		if len(item) == 6 && item[tableSpecsAttr] != nil && item[tableSpecsAttr].S != nil {
			return true, true
		}
		return len(item) == 5, false
	}
	return false, false
}

func (dm dynamoManifest) Update(lastLock, newLock addr, specs []tableSpec, newRoot hash.Hash, writeHook func()) (lock addr, actual hash.Hash, tableSpecs []tableSpec) {
	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(dm.table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:      {S: aws.String(dm.db)},
			nbsVersAttr: {S: aws.String(StorageVersion)},
			versAttr:    {S: aws.String(constants.NomsVersion)},
			rootAttr:    {B: newRoot[:]},
			lockAttr:    {B: newLock[:]},
		},
	}
	if len(specs) > 0 {
		tableInfo := make([]string, 2*len(specs))
		formatSpecs(specs, tableInfo)
		putArgs.Item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(strings.Join(tableInfo, ":"))}
	}

	expr := valueEqualsExpression
	if lastLock == (addr{}) {
		expr = valueNotExistsOrEqualsExpression
	}

	putArgs.ConditionExpression = aws.String(expr)
	putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":prev": {B: lastLock[:]},
		":vers": {S: aws.String(constants.NomsVersion)},
	}

	_, ddberr := dm.ddbsvc.PutItem(&putArgs)
	if ddberr != nil {
		if awsErr, ok := ddberr.(awserr.Error); ok {
			if awsErr.Code() == "ConditionalCheckFailedException" {
				exists, vers, lock, actual, tableSpecs := dm.ParseIfExists(nil)
				d.Chk.True(exists)
				d.Chk.True(vers == constants.NomsVersion)
				return lock, actual, tableSpecs
			} // TODO handle other aws errors?
		}
		d.Chk.NoError(ddberr)
	}

	return newLock, newRoot, specs
}
