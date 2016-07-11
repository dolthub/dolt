// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/suite"
)

type testValue struct {
	value       Value
	expectedRef string
	description string
}

type testSuite struct {
	suite.Suite
	testValues []*testValue
}

// please update Go and JS to keep them in sync - see js/src//xp-test.js
func newTestSuite() *testSuite {
	testValues := []*testValue{
		&testValue{Bool(true), "sha1-3f29546453678b855931c174a97d6c0894b8f546", "bool - true"},
		&testValue{Bool(false), "sha1-1489f923c4dca729178b3e3233458550d8dddf29", "bool - false"},
		&testValue{Number(-1), "sha1-47ec8d98366433dc002e7721c9e37d5067547937", "num - -1"},
		&testValue{Number(0), "sha1-9508e90548b0440a4a61e5743b76c1e309b23b7f", "num - 0"},
		&testValue{Number(1), "sha1-9f36f27018671b24dcdf70c9eb857d5ea2a064c8", "num - 1"},
		&testValue{String(""), "sha1-e1bc1dae59f116abb43f9dafbb2acc9b141aa6b0", "str - empty"},
		&testValue{String("0"), "sha1-a1c90c71d1ffdb51138677c578e6f2e8a011070d", "str - 0"},
		&testValue{String("false"), "sha1-e15d53dc6c9d3aa6eca4eea28382c9c45ba8fd9e", "str - false"},
	}

	// TODO: add these types too
	/*
		BlobKind
		ValueKind
		ListKind
		MapKind
		RefKind
		SetKind
		StructKind
		TypeKind
		CycleKind // Only used in encoding/decoding.
		UnionKind
	*/

	return &testSuite{testValues: testValues}
}

// write a value, read that value back out
// assert the values are equal and
// verify the digest is what we expect
func (suite *testSuite) roundTripDigestTest(t *testValue) {
	vs := NewTestValueStore()
	r := vs.WriteValue(t.value)
	v2 := vs.ReadValue(r.TargetHash())

	suite.True(v2.Equals(t.value), t.description)
	suite.True(t.value.Equals(v2), t.description)
	suite.Equal(t.expectedRef, r.TargetHash().String(), t.description)
}

// Called from testify suite.Run()
func (suite *testSuite) TestTypes() {
	for i := range suite.testValues {
		suite.roundTripDigestTest(suite.testValues[i])
	}
}

// Called from "go test"
func TestSuite(t *testing.T) {
	suite.Run(t, newTestSuite())
}
