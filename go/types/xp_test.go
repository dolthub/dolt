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
		&testValue{Bool(true), "g19moobgrm32dn083bokhksuobulq28c", "bool - true"},
		&testValue{Bool(false), "bqjhrhmgmjqnnssqln87o84c6no6pklq", "bool - false"},
		&testValue{Number(-1), "hq0jvv1enraehfggfk8s27ll1rmirt96", "num - -1"},
		&testValue{Number(0), "elie88b5iouak7onvi2mpkcgoqqr771l", "num - 0"},
		&testValue{Number(1), "6h9ldndhjoq0r5sbn1955gaearq5dovc", "num - 1"},
		&testValue{String(""), "ssfs0o2eq3kg50p37q2crhhqhjcs2391", "str - empty"},
		&testValue{String("0"), "jngc7d11d2h0c6s2f15l10rckvu753rb", "str - 0"},
		&testValue{String("false"), "1v3a1t4to25kkohm1bhh2thebmls0lp0", "str - false"},
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
