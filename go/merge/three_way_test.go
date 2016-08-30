// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
)

type seq interface {
	items() []interface{}
}

type ThreeWayMergeSuite struct {
	suite.Suite
	create  func(seq) types.Value
	typeStr string
}

func (s *ThreeWayMergeSuite) tryThreeWayMerge(a, b, p, exp seq, vs types.ValueReadWriter) {
	merged, err := ThreeWay(s.create(a), s.create(b), s.create(p), vs)
	if s.NoError(err) {
		expected := s.create(exp)
		s.True(expected.Equals(merged), "%s != %s", types.EncodedValue(expected), types.EncodedValue(merged))
	}
}

func (s *ThreeWayMergeSuite) tryThreeWayConflict(a, b, p types.Value, contained string) {
	m, err := ThreeWay(a, b, p, nil)
	if s.Error(err) {
		s.Contains(err.Error(), contained)
		return
	}
	s.Fail("Expected error!", "Got successful merge: %s", types.EncodedValue(m))
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
		v1 = types.Number(t)
	case seq:
		v1 = f(t)
	case types.Value:
		v1 = t
	}
	return v1
}
