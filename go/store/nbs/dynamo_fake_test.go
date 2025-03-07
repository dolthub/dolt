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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/constants"
)

type fakeDDB struct {
	data             map[string]interface{}
	t                *testing.T
	numPuts, numGets int64
}

type record struct {
	lock, root            []byte
	vers, specs, appendix string
}

func makeFakeDDB(t *testing.T) *fakeDDB {
	return &fakeDDB{
		data: map[string]interface{}{},
		t:    t,
	}
}

func (m *fakeDDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	keyM := input.Key[dbAttr].(*ddbtypes.AttributeValueMemberS)
	assert.NotNil(m.t, keyM, "key should have been a String: %+v", input.Key[dbAttr])

	item := map[string]ddbtypes.AttributeValue{}
	if e, present := m.data[keyM.Value]; present {
		item[dbAttr] = &ddbtypes.AttributeValueMemberS{Value: keyM.Value}
		switch e := e.(type) {
		case record:
			item[nbsVersAttr] = &ddbtypes.AttributeValueMemberS{Value: AWSStorageVersion}
			item[versAttr] = &ddbtypes.AttributeValueMemberS{Value: e.vers}
			item[rootAttr] = &ddbtypes.AttributeValueMemberB{Value: e.root}
			item[lockAttr] = &ddbtypes.AttributeValueMemberB{Value: e.lock}
			if e.specs != "" {
				item[tableSpecsAttr] = &ddbtypes.AttributeValueMemberS{Value: e.specs}
			}
			if e.appendix != "" {
				item[appendixAttr] = &ddbtypes.AttributeValueMemberS{Value: e.appendix}
			}
		}
	}
	atomic.AddInt64(&m.numGets, 1)
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *fakeDDB) putRecord(k string, l, r []byte, v string, s string, a string) {
	m.data[k] = record{l, r, v, s, a}
}

func (m *fakeDDB) putData(k string, d []byte) {
	m.data[k] = d
}

func (m *fakeDDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	require.NotNil(m.t, input.Item[dbAttr], "%s should have been present", dbAttr)
	require.IsType(m.t, (*ddbtypes.AttributeValueMemberS)(nil), input.Item[dbAttr], "key should have been a String: %+v", input.Item[dbAttr])
	key := input.Item[dbAttr].(*ddbtypes.AttributeValueMemberS).Value

	require.NotNil(m.t, input.Item[nbsVersAttr], "%s should have been present", nbsVersAttr)
	require.IsType(m.t, (*ddbtypes.AttributeValueMemberS)(nil), input.Item[nbsVersAttr], "nbsVers should have been a String: %+v", input.Item[nbsVersAttr])
	assert.Equal(m.t, AWSStorageVersion, input.Item[nbsVersAttr].(*ddbtypes.AttributeValueMemberS).Value)

	require.NotNil(m.t, input.Item[versAttr], "%s should have been present", versAttr)
	require.IsType(m.t, (*ddbtypes.AttributeValueMemberS)(nil), input.Item[versAttr], "nbsVers should have been a String: %+v", input.Item[versAttr])
	assert.Equal(m.t, constants.FormatLD1String, input.Item[versAttr].(*ddbtypes.AttributeValueMemberS).Value)

	require.NotNil(m.t, input.Item[lockAttr], "%s should have been present", lockAttr)
	require.IsType(m.t, (*ddbtypes.AttributeValueMemberB)(nil), input.Item[lockAttr], "lock should have been a blob: %+v", input.Item[lockAttr])
	lock := input.Item[lockAttr].(*ddbtypes.AttributeValueMemberB).Value

	require.NotNil(m.t, input.Item[rootAttr], "%s should have been present", rootAttr)
	require.IsType(m.t, (*ddbtypes.AttributeValueMemberB)(nil), input.Item[rootAttr], "root should have been a blob: %+v", input.Item[rootAttr])
	root := input.Item[rootAttr].(*ddbtypes.AttributeValueMemberB).Value

	specs := ""
	if attr, present := input.Item[tableSpecsAttr]; present {
		require.IsType(m.t, (*ddbtypes.AttributeValueMemberS)(nil), attr, "specs should have been a String: %+v", input.Item[tableSpecsAttr])
		specs = attr.(*ddbtypes.AttributeValueMemberS).Value
	}

	apps := ""
	if attr, present := input.Item[appendixAttr]; present {
		require.IsType(m.t, (*ddbtypes.AttributeValueMemberS)(nil), attr, "appendix specs should have been a String: %+v", input.Item[appendixAttr])
		apps = attr.(*ddbtypes.AttributeValueMemberS).Value
	}

	mustNotExist := *(input.ConditionExpression) == valueNotExistsOrEqualsExpression
	current, present := m.data[key]

	if mustNotExist && present {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	} else if !mustNotExist && !checkCondition(current.(record), input.ExpressionAttributeValues) {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	}

	m.putRecord(key, lock, root, constants.FormatLD1String, specs, apps)

	atomic.AddInt64(&m.numPuts, 1)
	return &dynamodb.PutItemOutput{}, nil
}

func checkCondition(current record, expressionAttrVals map[string]ddbtypes.AttributeValue) bool {
	return current.vers == expressionAttrVals[versExpressionValuesKey].(*ddbtypes.AttributeValueMemberS).Value && bytes.Equal(current.lock, expressionAttrVals[prevLockExpressionValuesKey].(*ddbtypes.AttributeValueMemberB).Value)

}

func (m *fakeDDB) NumGets() int64 {
	return atomic.LoadInt64(&m.numGets)
}
