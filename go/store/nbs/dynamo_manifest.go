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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

type DynamoDBAPIV2 interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// dynamoManifest assumes the existence of a DynamoDB table whose primary partition key is in String format and named `db`.
type dynamoManifest struct {
	table, db string
	ddbsvc    DynamoDBAPIV2
}

func newDynamoManifest(table, namespace string, ddb DynamoDBAPIV2) manifest {
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

	result, err := dm.ddbsvc.GetItem(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(dm.table),
		Key: map[string]ddbtypes.AttributeValue{
			dbAttr: &ddbtypes.AttributeValueMemberS{
				Value: dm.db,
			},
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
		contents.nbfVers = result.Item[versAttr].(*ddbtypes.AttributeValueMemberS).Value
		contents.root = hash.New(result.Item[rootAttr].(*ddbtypes.AttributeValueMemberB).Value)
		copy(contents.lock[:], result.Item[lockAttr].(*ddbtypes.AttributeValueMemberB).Value)
		if hasSpecs {
			contents.specs, err = parseSpecs(strings.Split(result.Item[tableSpecsAttr].(*ddbtypes.AttributeValueMemberS).Value, ":"))
			if err != nil {
				return false, manifestContents{}, ErrCorruptManifest
			}
		}

		if hasAppendix {
			contents.appendix, err = parseSpecs(strings.Split(result.Item[appendixAttr].(*ddbtypes.AttributeValueMemberS).Value, ":"))
			if err != nil {
				return false, manifestContents{}, ErrCorruptManifest
			}
		}
	}

	return exists, contents, nil
}

func validateManifest(item map[string]ddbtypes.AttributeValue) (valid, hasSpecs, hasAppendix bool) {
	if nbsVersA := item[nbsVersAttr]; nbsVersA == nil {
		return false, false, false
	} else if nbsVers, ok := nbsVersA.(*ddbtypes.AttributeValueMemberS); !ok {
		return false, false, false
	} else if nbsVers.Value != AWSStorageVersion {
		return false, false, false
	}
	if versA := item[versAttr]; versA == nil {
		return false, false, false
	} else if _, ok := versA.(*ddbtypes.AttributeValueMemberS); !ok {
		return false, false, false
	}
	if lockA := item[lockAttr]; lockA == nil {
		return false, false, false
	} else if _, ok := lockA.(*ddbtypes.AttributeValueMemberB); !ok {
		return false, false, false
	}
	if rootA := item[rootAttr]; rootA == nil {
		return false, false, false
	} else if _, ok := rootA.(*ddbtypes.AttributeValueMemberB); !ok {
		return false, false, false
	}
	if len(item) == 6 || len(item) == 7 {
		if tableSpecsA := item[tableSpecsAttr]; tableSpecsA == nil {
		} else if _, ok := tableSpecsA.(*ddbtypes.AttributeValueMemberS); ok {
			hasSpecs = true
		}
		if appendixA := item[appendixAttr]; appendixA == nil {
		} else if _, ok := appendixA.(*ddbtypes.AttributeValueMemberS); ok {
			hasAppendix = true
		}
		return true, hasSpecs, hasAppendix
	}
	return len(item) == 5, false, false
}

func (dm dynamoManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()

	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(dm.table),
		Item: map[string]ddbtypes.AttributeValue{
			dbAttr:      &ddbtypes.AttributeValueMemberS{Value: dm.db},
			nbsVersAttr: &ddbtypes.AttributeValueMemberS{Value: AWSStorageVersion},
			versAttr:    &ddbtypes.AttributeValueMemberS{Value: newContents.nbfVers},
			rootAttr:    &ddbtypes.AttributeValueMemberB{Value: newContents.root[:]},
			lockAttr:    &ddbtypes.AttributeValueMemberB{Value: newContents.lock[:]},
		},
	}

	if len(newContents.specs) > 0 {
		tableInfo := make([]string, 2*len(newContents.specs))
		formatSpecs(newContents.specs, tableInfo)
		putArgs.Item[tableSpecsAttr] = &ddbtypes.AttributeValueMemberS{Value: strings.Join(tableInfo, ":")}
	}

	if len(newContents.appendix) > 0 {
		tableInfo := make([]string, 2*len(newContents.appendix))
		formatSpecs(newContents.appendix, tableInfo)
		putArgs.Item[appendixAttr] = &ddbtypes.AttributeValueMemberS{Value: strings.Join(tableInfo, ":")}
	}

	expr := valueEqualsExpression
	if lastLock.IsEmpty() {
		expr = valueNotExistsOrEqualsExpression
	}

	putArgs.ConditionExpression = aws.String(expr)
	putArgs.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{
		prevLockExpressionValuesKey: &ddbtypes.AttributeValueMemberB{Value: lastLock[:]},
		versExpressionValuesKey:     &ddbtypes.AttributeValueMemberS{Value: newContents.nbfVers},
	}

	_, ddberr := dm.ddbsvc.PutItem(ctx, &putArgs)
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
	var ccfe *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccfe)
}
