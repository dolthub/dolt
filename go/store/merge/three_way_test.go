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

package merge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

type seq interface {
	items() []interface{}
}

type ThreeWayMergeSuite struct {
	suite.Suite
	vs      *types.ValueStore
	create  func(seq) (types.Value, error)
	typeStr string
}

func (s *ThreeWayMergeSuite) SetupTest() {
	storage := &chunks.MemoryStorage{}
	s.vs = types.NewValueStore(storage.NewViewWithDefaultFormat())
}

func (s *ThreeWayMergeSuite) TearDownTest() {
	err := s.vs.Close()
	s.NoError(err)
}

func (s *ThreeWayMergeSuite) tryThreeWayMerge(a, b, p, exp seq) {
	aVal, err := s.create(a)
	s.NoError(err)
	bVal, err := s.create(b)
	s.NoError(err)
	pVal, err := s.create(p)
	s.NoError(err)
	merged, err := ThreeWay(context.Background(), aVal, bVal, pVal, s.vs, nil, nil)
	if s.NoError(err) {
		expected, err := s.create(exp)
		s.NoError(err)
		s.True(expected.Equals(merged), "%s != %s", mustString(types.EncodedValue(context.Background(), expected)), mustString(types.EncodedValue(context.Background(), merged)))
	}
}

func (s *ThreeWayMergeSuite) tryThreeWayConflict(a, b, p types.Value, contained string) {
	m, err := ThreeWay(context.Background(), a, b, p, s.vs, nil, nil)
	if s.Error(err) {
		s.Contains(err.Error(), contained)
		return
	}
	s.Fail("Expected error!", "Got successful merge: %s", mustString(types.EncodedValue(context.Background(), m)))
}

func valsToTypesValues(f func(seq) (types.Value, error), items ...interface{}) ([]types.Value, error) {
	keyValues := []types.Value{}
	for _, e := range items {
		v, err := valToTypesValue(f, e)

		if err != nil {
			return nil, err
		}

		keyValues = append(keyValues, v)
	}
	return keyValues, nil
}

func valToTypesValue(f func(seq) (types.Value, error), v interface{}) (types.Value, error) {
	var v1 types.Value
	var err error
	switch t := v.(type) {
	case string:
		v1 = types.String(t)
	case int:
		v1 = types.Float(t)
	case seq:
		v1, err = f(t)
	case types.Value:
		v1 = t
	}
	return v1, err
}

func TestThreeWayMerge_PrimitiveConflict(t *testing.T) {
	threeWayConflict := func(a, b, p types.Value, contained string) {
		mrgr := &merger{}
		m, err := mrgr.threeWay(context.Background(), a, b, p, nil)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), contained)
			return
		}
		assert.Fail(t, "Expected error!", "Got successful merge: %s", mustString(types.EncodedValue(context.Background(), m)))
	}

	a, b, p := types.Float(7), types.String("nope"), types.String("parent")

	threeWayConflict(a, b, p, "Float and String on top of")
	threeWayConflict(b, a, p, "String and Float on top of")
}
