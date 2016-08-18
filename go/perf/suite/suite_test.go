// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package suite

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

type testSuite struct {
	PerfSuite
	tempFileName, tempDir     string
	setupTest, tearDownTest   int
	setupRep, tearDownRep     int
	setupSuite, tearDownSuite int
}

// This is the only test that does anything interesting. The others are just to test naming.
func (s *testSuite) TestInterestingStuff() {
	assert := s.NewAssert()
	assert.NotNil(s.T)
	assert.NotNil(s.W)
	assert.NotEqual("", s.AtticLabs)
	assert.NotEqual("", s.DatabaseSpec)

	val := types.Bool(true)
	r := s.Database.WriteValue(val)
	assert.True(s.Database.ReadValue(r.TargetHash()).Equals(val))

	s.Pause(func() {
		s.waitForSmidge()
	})

	s.tempFileName = s.TempFile("suite.suite_test").Name()
	s.tempDir = s.TempDir("suite.suite_test")
}

func (s *testSuite) TestFoo() {
	s.waitForSmidge()
}

func (s *testSuite) TestBar() {
	s.waitForSmidge()
}

func (s *testSuite) Test01Abc() {
	s.waitForSmidge()
}

func (s *testSuite) Test02Def() {
	s.waitForSmidge()
}

func (s *testSuite) testNothing() {
	s.waitForSmidge()
}

func (s *testSuite) Testimate() {
	s.waitForSmidge()
}

func (s *testSuite) SetupTest() {
	s.setupTest++
}

func (s *testSuite) TearDownTest() {
	s.tearDownTest++
}

func (s *testSuite) SetupRep() {
	s.setupRep++
}

func (s *testSuite) TearDownRep() {
	s.tearDownRep++
}

func (s *testSuite) SetupSuite() {
	s.setupSuite++
}

func (s *testSuite) TearDownSuite() {
	s.tearDownSuite++
}

func (s *testSuite) waitForSmidge() {
	// Tests should call this to make sure the measurement shows up as > 0, not that it shows up as a millisecond.
	<-time.After(time.Millisecond)
}

func TestSuite(t *testing.T) {
	runTestSuite(t, false)
}

func TestSuiteWithMem(t *testing.T) {
	runTestSuite(t, true)
}

func runTestSuite(t *testing.T, mem bool) {
	assert := assert.New(t)

	// Write test results to our own temporary LDB database.
	ldbDir, err := ioutil.TempDir("", "suite.TestSuite")
	assert.NoError(err)
	defer os.RemoveAll(ldbDir)

	perfFlagVal := *perfFlag
	perfRepeatFlagVal := *perfRepeatFlag
	perfMemFlagVal := *perfMemFlag
	*perfFlag = ldbDir
	*perfRepeatFlag = 3
	*perfMemFlag = mem
	defer func() {
		*perfFlag = perfFlagVal
		*perfRepeatFlag = perfRepeatFlagVal
		*perfMemFlag = perfMemFlagVal
	}()

	s := &testSuite{}
	Run("ds", t, s)

	expectedTests := []string{"Abc", "Bar", "Def", "Foo", "InterestingStuff"}

	// The temp file and dir should have been cleaned up.
	_, err = os.Stat(s.tempFileName)
	assert.NotNil(err)
	_, err = os.Stat(s.tempDir)
	assert.NotNil(err)

	// The correct number of Setup/TearDown calls should have been run.
	assert.Equal(1, s.setupSuite)
	assert.Equal(1, s.tearDownSuite)
	assert.Equal(*perfRepeatFlag, s.setupRep)
	assert.Equal(*perfRepeatFlag, s.tearDownRep)
	assert.Equal(*perfRepeatFlag*len(expectedTests), s.setupTest)
	assert.Equal(*perfRepeatFlag*len(expectedTests), s.tearDownTest)

	// The results should have been written to the "ds" dataset.
	ds, err := spec.GetDataset(ldbDir + "::ds")
	assert.NoError(err)
	head := ds.HeadValue().(types.Struct)

	// These tests mostly assert that the structure of the results is correct. Specific values are hard.

	getOrFail := func(s types.Struct, f string) types.Value {
		val, ok := s.MaybeGet(f)
		assert.True(ok)
		return val
	}

	env, ok := getOrFail(head, "environment").(types.Struct)
	assert.True(ok)

	getOrFail(env, "diskUsages")
	getOrFail(env, "cpus")
	getOrFail(env, "mem")
	getOrFail(env, "host")
	getOrFail(env, "partitions")

	nomsRevision := getOrFail(head, "nomsRevision")
	assert.True(ok)
	assert.True(string(nomsRevision.(types.String)) != "")
	getOrFail(head, "testdataRevision")

	reps, ok := getOrFail(head, "reps").(types.List)
	assert.True(ok)
	assert.Equal(*perfRepeatFlag, int(reps.Len()))

	reps.IterAll(func(rep types.Value, _ uint64) {
		i := 0

		rep.(types.Map).IterAll(func(k, timesVal types.Value) {
			assert.True(i < len(expectedTests))
			assert.Equal(expectedTests[i], string(k.(types.String)))

			times := timesVal.(types.Struct)
			assert.True(getOrFail(times, "elapsed").(types.Number) > 0)
			assert.True(getOrFail(times, "total").(types.Number) > 0)

			paused := getOrFail(times, "paused").(types.Number)
			if k == types.String("InterestingStuff") {
				assert.True(paused > 0)
			} else {
				assert.True(paused == 0)
			}

			i++
		})

		assert.Equal(i, len(expectedTests))
	})
}
