package types

import (
	"github.com/stretchr/testify/suite"
	"testing"
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
		&testValue{Bool(true), "sha1-b6c4dd02a2f17ae9693627f03f642b988b4d5b63", "bool - true"},
		&testValue{Bool(false), "sha1-dd1259720743f53a411788282c556662db14c758", "bool - false"},
		&testValue{Number(-1), "sha1-4cff7171b2664044dc02d304e8aba7fc733681a0", "num - -1"},
		&testValue{Number(0), "sha1-99b6938ab3aa497b1392fdbcb34b63bf4fe75c3c", "num - 0"},
		&testValue{Number(1), "sha1-fef7b450ff9b1e5a34dbfa9702bb78ebff1c2730", "num - 1"},
		&testValue{NewString(""), "sha1-9f4895d88ceab0d09962d84f6d5a93d3451ae9a3", "str - empty"},
		&testValue{NewString("0"), "sha1-e557fdd1c0b2661daac19b40446ffd4bafde793a", "str - 0"},
		&testValue{NewString("false"), "sha1-9fe813b27cf8ae1ca5d258c5299caa4f749e86c4", "str - false"},
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
		ParentKind // Only used in encoding/decoding.
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
