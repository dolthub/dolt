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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	// DynamoManifest does not yet include GC Generation
	AWSStorageVersion = "4"

	dbAttr                      = "db"
	lockAttr                    = "lck" // 'lock' is a reserved word in dynamo
	rootAttr                    = "root"
	versAttr                    = "vers"
	nbsVersAttr                 = "nbsVers"
	tableSpecsAttr              = "specs"
	appendixAttr                = "appendix"
	prevLockExpressionValuesKey = ":prev"
	versExpressionValuesKey     = ":vers"
)

var (
	valueEqualsExpression            = fmt.Sprintf("(%s = %s) and (%s = %s)", lockAttr, prevLockExpressionValuesKey, versAttr, versExpressionValuesKey)
	valueNotExistsOrEqualsExpression = fmt.Sprintf("attribute_not_exists("+lockAttr+") or %s", valueEqualsExpression)
)

type ddbsvc interface {
	GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error)
	PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error)
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

func (dm dynamoManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTimeSince(t1) }()

	var exists bool
	var contents manifestContents

	result, err := dm.ddbsvc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true), // This doubles the cost :-(
		TableName:      aws.String(dm.table),
		Key: map[string]*dynamodb.AttributeValue{
			dbAttr: {S: aws.String(dm.db)},
		},
	})

	if err != nil {
		return false, manifestContents{}, fmt.Errorf("failed to get dynamo table: '%s' - %w", dm.table, err)
	}

	// !exists(dbAttr) => uninitialized store
	if len(result.Item) > 0 {
		valid, hasSpecs, hasAppendix := validateManifest(result.Item)
		if !valid {
			return false, contents, ErrCorruptManifest
		}

		exists = true
		contents.nbfVers = *result.Item[versAttr].S
		contents.root = hash.New(result.Item[rootAttr].B)
		copy(contents.lock[:], result.Item[lockAttr].B)
		if hasSpecs {
			contents.specs, err = parseSpecs(strings.Split(*result.Item[tableSpecsAttr].S, ":"))
			if err != nil {
				return false, manifestContents{}, ErrCorruptManifest
			}
		}

		if hasAppendix {
			contents.appendix, err = parseSpecs(strings.Split(*result.Item[appendixAttr].S, ":"))
			if err != nil {
				return false, manifestContents{}, ErrCorruptManifest
			}
		}
	}

	return exists, contents, nil
}

func validateManifest(item map[string]*dynamodb.AttributeValue) (valid, hasSpecs, hasAppendix bool) {
	if item[nbsVersAttr] != nil && item[nbsVersAttr].S != nil &&
		AWSStorageVersion == *item[nbsVersAttr].S &&
		item[versAttr] != nil && item[versAttr].S != nil &&
		item[lockAttr] != nil && item[lockAttr].B != nil &&
		item[rootAttr] != nil && item[rootAttr].B != nil {
		if len(item) == 6 || len(item) == 7 {
			if item[tableSpecsAttr] != nil && item[tableSpecsAttr].S != nil {
				hasSpecs = true
			}
			if item[appendixAttr] != nil && item[appendixAttr].S != nil {
				hasAppendix = true
			}
			return true, hasSpecs, hasAppendix
		}
		return len(item) == 5, false, false
	}
	return false, false, false
}

func (dm dynamoManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(dm.table),
		Item: map[string]*dynamodb.AttributeValue{
			dbAttr:      {S: aws.String(dm.db)},
			nbsVersAttr: {S: aws.String(AWSStorageVersion)},
			versAttr:    {S: aws.String(newContents.nbfVers)},
			rootAttr:    {B: newContents.root[:]},
			lockAttr:    {B: newContents.lock[:]},
		},
	}

	if len(newContents.specs) > 0 {
		tableInfo := make([]string, 2*len(newContents.specs))
		formatSpecs(newContents.specs, tableInfo)
		putArgs.Item[tableSpecsAttr] = &dynamodb.AttributeValue{S: aws.String(strings.Join(tableInfo, ":"))}
	}

	if len(newContents.appendix) > 0 {
		tableInfo := make([]string, 2*len(newContents.appendix))
		formatSpecs(newContents.appendix, tableInfo)
		putArgs.Item[appendixAttr] = &dynamodb.AttributeValue{S: aws.String(strings.Join(tableInfo, ":"))}
	}

	expr := valueEqualsExpression
	if lastLock.IsEmpty() {
		expr = valueNotExistsOrEqualsExpression
	}

	putArgs.ConditionExpression = aws.String(expr)
	putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		prevLockExpressionValuesKey: {B: lastLock[:]},
		versExpressionValuesKey:     {S: aws.String(newContents.nbfVers)},
	}

	_, ddberr := dm.ddbsvc.PutItemWithContext(ctx, &putArgs)
	if ddberr != nil {
		if errIsConditionalCheckFailed(ddberr) {
			exists, upstream, err := dm.ParseIfExists(ctx, stats, nil)

			if err != nil {
				return manifestContents{}, err
			}

			if !exists {
				return manifestContents{}, errors.New("manifest not found")
			}

			if upstream.nbfVers != newContents.nbfVers {
				return manifestContents{}, errors.New("version mismatch")
			}

			return upstream, nil
		}

		if ddberr != nil {
			return manifestContents{}, ddberr
		}
	}

	return newContents, nil
}

func errIsConditionalCheckFailed(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && awsErr.Code() == "ConditionalCheckFailedException"
}
