// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	dbAttr         = "db"
	lockAttr       = "lck" // 'lock' is a reserved word in dynamo
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

// dynamoManifest assumes the existence of a DynamoDB table whose primary partition key is in String format and named `db`.
type dynamoManifest struct {
	table, db string
	ddbsvc    ddbsvc
}

func newDynamoManifest(table, namespace string, ddb ddbsvc) manifest {
	d.PanicIfTrue(table == "")
	d.PanicIfTrue(namespace == "")
	return dynamoManifest{table, namespace, ddb}
}

func (dm dynamoManifest) Name() string {
	return dm.table + dm.db
}

func (dm dynamoManifest) ParseIfExists(stats *Stats, readHook func()) (exists bool, contents manifestContents) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTimeSince(t1) }()

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
		contents.vers = *result.Item[versAttr].S
		contents.root = hash.New(result.Item[rootAttr].B)
		copy(contents.lock[:], result.Item[lockAttr].B)
		if hasSpecs {
			contents.specs = parseSpecs(strings.Split(*result.Item[tableSpecsAttr].S, ":"))
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

func (dm dynamoManifest) Update(lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(dm.table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:      {S: aws.String(dm.db)},
			nbsVersAttr: {S: aws.String(StorageVersion)},
			versAttr:    {S: aws.String(newContents.vers)},
			rootAttr:    {B: newContents.root[:]},
			lockAttr:    {B: newContents.lock[:]},
		},
	}
	if len(newContents.specs) > 0 {
		tableInfo := make([]string, 2*len(newContents.specs))
		formatSpecs(newContents.specs, tableInfo)
		putArgs.Item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(strings.Join(tableInfo, ":"))}
	}

	expr := valueEqualsExpression
	if lastLock == (addr{}) {
		expr = valueNotExistsOrEqualsExpression
	}

	putArgs.ConditionExpression = aws.String(expr)
	putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":prev": {B: lastLock[:]},
		":vers": {S: aws.String(newContents.vers)},
	}

	_, ddberr := dm.ddbsvc.PutItem(&putArgs)
	if ddberr != nil {
		if errIsConditionalCheckFailed(ddberr) {
			exists, upstream := dm.ParseIfExists(stats, nil)
			d.Chk.True(exists)
			d.Chk.True(upstream.vers == constants.NomsVersion)
			return upstream
		} // TODO handle other aws errors?
		d.PanicIfError(ddberr)
	}

	return newContents
}

func errIsConditionalCheckFailed(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && awsErr.Code() == "ConditionalCheckFailedException"
}
