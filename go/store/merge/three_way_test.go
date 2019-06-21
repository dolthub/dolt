// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/go/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type seq interface {
	items() []interface{}
}

type ThreeWayMergeSuite struct {
	suite.Suite
	vs      *types.ValueStore
	create  func(seq) types.Value
	typeStr string
}

func (s *ThreeWayMergeSuite) SetupTest() {
	storage := &chunks.MemoryStorage{}
	s.vs = types.NewValueStore(storage.NewView())
}

func (s *ThreeWayMergeSuite) TearDownTest() {
	s.vs.Close()
}

func (s *ThreeWayMergeSuite) tryThreeWayMerge(a, b, p, exp seq) {
	merged, err := ThreeWay(context.Background(), s.create(a), s.create(b), s.create(p), s.vs, nil, nil)
	if s.NoError(err) {
		expected := s.create(exp)
		s.True(expected.Equals(merged), "%s != %s", types.EncodedValue(context.Background(), expected), types.EncodedValue(context.Background(), merged))
	}
}

func (s *ThreeWayMergeSuite) tryThreeWayConflict(a, b, p types.Value, contained string) {
	m, err := ThreeWay(context.Background(), a, b, p, s.vs, nil, nil)
	if s.Error(err) {
		s.Contains(err.Error(), contained)
		return
	}
	s.Fail("Expected error!", "Got successful merge: %s", types.EncodedValue(context.Background(), m))
}

func valsToTypesValues(f func(seq) types.Value, items ...interface{}) []types.Value {
	keyValues := []types.Value{}
	for _, e := range items {
		v := valToTypesValue(f, e)
		keyValues = append(keyValues, v)
	}
	return keyValues
}

func valToTypesValue(f func(seq) types.Value, v interface{}) types.Value {
	var v1 types.Value
	switch t := v.(type) {
	case string:
		v1 = types.String(t)
	case int:
		v1 = types.Float(t)
	case seq:
		v1 = f(t)
	case types.Value:
		v1 = t
	}
	return v1
}

func TestThreeWayMerge_PrimitiveConflict(t *testing.T) {
	threeWayConflict := func(a, b, p types.Value, contained string) {
		mrgr := &merger{}
		m, err := mrgr.threeWay(context.Background(), a, b, p, nil)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), contained)
			return
		}
		assert.Fail(t, "Expected error!", "Got successful merge: %s", types.EncodedValue(context.Background(), m))
	}

	a, b, p := types.Float(7), types.String("nope"), types.String("parent")

	threeWayConflict(a, b, p, "Float and String on top of")
	threeWayConflict(b, a, p, "String and Float on top of")
}
