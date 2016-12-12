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
	rootAttr       = "root"
	versAttr       = "vers"
	nbsVersAttr    = "nbsVers"
	tableSpecsAttr = "specs"
)

var (
	valueEqualsExpression            = fmt.Sprintf("(%s = :prev) and (%s = :vers)", rootAttr, versAttr)
	valueNotExistsOrEqualsExpression = fmt.Sprintf("attribute_not_exists("+rootAttr+") or %s", valueEqualsExpression)
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

func newDynamoManifest(table, namespace string, ddb ddbsvc) *dynamoManifest {
	return &dynamoManifest{table: table, db: namespace, ddbsvc: ddb}
}

func (dm dynamoManifest) ParseIfExists(readHook func()) (exists bool, vers string, root hash.Hash, tableSpecs []tableSpec) {
	result, err := dm.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dm.table),
		Key: map[string]*dynamodb.AttributeValue{
			dbAttr: {S: aws.String(dm.db)},
		},
	})
	d.PanicIfError(err)

	// !exists(dbAttr) => unitialized store
	if len(result.Item) > 0 {
		if !validateManifest(result.Item) {
			d.Panic("Malformed manifest for %s: %+v", dm.db, result.Item)
		}
		exists = true
		vers = *result.Item[versAttr].S
		root = hash.New(result.Item[rootAttr].B)
		tableSpecs = parseSpecs(strings.Split(*result.Item[tableSpecsAttr].S, ":"))
	}

	return
}

func validateManifest(item map[string]*dynamodb.AttributeValue) bool {
	return len(item) == 5 &&
		item[nbsVersAttr] != nil && item[nbsVersAttr].S != nil &&
		StorageVersion == *item[nbsVersAttr].S &&
		item[versAttr] != nil && item[versAttr].S != nil &&
		item[rootAttr] != nil && item[rootAttr].B != nil &&
		item[tableSpecsAttr] != nil && item[tableSpecsAttr].S != nil
}

func (dm dynamoManifest) Update(specs []tableSpec, root, newRoot hash.Hash, writeHook func()) (actual hash.Hash, tableSpecs []tableSpec) {
	tableSpecs = specs

	tableInfo := make([]string, 2*len(tableSpecs))
	formatSpecs(tableSpecs, tableInfo)
	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(dm.table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:         {S: aws.String(dm.db)},
			nbsVersAttr:    {S: aws.String(StorageVersion)},
			versAttr:       {S: aws.String(constants.NomsVersion)},
			rootAttr:       {B: newRoot[:]},
			tableSpecsAttr: {S: aws.String(strings.Join(tableInfo, ":"))},
		},
	}

	expr := valueEqualsExpression
	if root.IsEmpty() {
		expr = valueNotExistsOrEqualsExpression
	}

	putArgs.ConditionExpression = aws.String(expr)
	putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":prev": {B: root[:]},
		":vers": {S: aws.String(constants.NomsVersion)},
	}

	_, err := dm.ddbsvc.PutItem(&putArgs)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ConditionalCheckFailedException" {
				exists, vers, actual, tableSpecs := dm.ParseIfExists(nil)
				d.Chk.True(exists)
				d.Chk.True(vers == constants.NomsVersion)
				return actual, tableSpecs
			} // TODO handle other aws errors?
		}
		d.Chk.NoError(err)
	}

	return newRoot, tableSpecs
}
