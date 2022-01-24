// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testId            = "test-id"
	testSuiteId       = "test-suite-id"
	testOS            = "test-runtime-os"
	testGoArch        = "test-runtime-goarch"
	testServer        = "test-server-name"
	testServerVersion = "test-version"
	testServerParams  = "--test-server-param=1"
	testTestName      = "test-generated-name"
	testTestParams    = "--test-param=2"
	testStamp         = time.Date(2019, 8, 19, 0, 0, 0, 0, time.UTC).Format(stampFormat)
)

// getRandomResult returns a result with random numbers
func genRandomResult() *Result {
	return &Result{
		RuntimeOS:                testOS,
		RuntimeGoArch:            testGoArch,
		ServerName:               testServer,
		ServerParams:             testServerParams,
		TestName:                 testTestName,
		TestParams:               testTestParams,
		CreatedAt:                testStamp,
		SqlReadQueries:           rand.Int63(),
		SqlWriteQueries:          rand.Int63(),
		SqlOtherQueries:          rand.Int63(),
		SqlTotalQueries:          rand.Int63(),
		SqlTotalQueriesPerSecond: rand.Float64(),
		TransactionsTotal:        rand.Int63(),
		TransactionsPerSecond:    rand.Float64(),
		IgnoredErrorsTotal:       rand.Int63(),
		IgnoredErrorsPerSecond:   rand.Float64(),
		ReconnectsTotal:          rand.Int63(),
		ReconnectsPerSecond:      rand.Float64(),
		TotalTimeSeconds:         rand.Float64(),
		TotalNumberOfEvents:      rand.Int63(),
		LatencyMinMS:             rand.Float64(),
		LatencyAvgMS:             rand.Float64(),
		LatencyMaxMS:             rand.Float64(),
		LatencyPercentile:        rand.Float64(),
		LatencySumMS:             rand.Float64(),
	}
}

func TestFromValWithParens(t *testing.T) {
	tests := []struct {
		description   string
		line          string
		expectedOne   string
		expectedTwo   string
		expectedError error
	}{
		{
			description: "should return no vals from empty line",
			line:        "",
			expectedOne: "",
			expectedTwo: "",
		},
		{
			description: "should return outside val from line without parens",
			line:        "        10123         ",
			expectedOne: "10123",
			expectedTwo: "",
		},
		{
			description: "should return outside val from line with parens",
			line:        "        708    ()",
			expectedOne: "708",
			expectedTwo: "",
		},
		{
			description: "should return inside val from line with parens",
			line:        "(1131.47 per sec.)",
			expectedOne: "",
			expectedTwo: "1131.47 per sec.",
		},
		{
			description: "should return vals from line with parens",
			line:        "                      0      (0.00 per sec.)",
			expectedOne: "0",
			expectedTwo: "0.00 per sec.",
		},
		{
			description:   "should error if line is incorrectly formatted",
			line:          "12 () 13 (24) 1 (255)",
			expectedOne:   "",
			expectedTwo:   "",
			expectedError: ErrUnableToParseOutput,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			actualOne, actualTwo, err := FromValWithParens(test.line)
			assert.Equal(t, test.expectedOne, actualOne)
			assert.Equal(t, test.expectedTwo, actualTwo)
			assert.Equal(t, test.expectedError, err)
		})
	}
}

func TestUpdateResults(t *testing.T) {
	tests := []struct {
		description    string
		line           string
		expectedResult *Result
		expectedError  error
	}{
		{
			description:    "should update read queries",
			line:           "  read:                            9912",
			expectedResult: &Result{SqlReadQueries: 9912},
		},
		{
			description:    "should update transactions",
			line:           "transactions:                        708    (70.72 per sec.)",
			expectedResult: &Result{TransactionsTotal: 708, TransactionsPerSecond: 70.72},
		},
		{
			description:    "should update total queries per second",
			line:           "queries:                             11328  (1131.47 per sec.)",
			expectedResult: &Result{SqlTotalQueriesPerSecond: 1131.47},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			actual := &Result{}
			err := UpdateResult(actual, test.line)
			assert.Equal(t, test.expectedResult, actual)
			assert.Equal(t, test.expectedError, err)
		})
	}
}

var sampleOutput1 = `
Running the test with following options:
Number of threads: 1
Initializing random number generator from seed (1).


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            9464
        write:                           0
        other:                           1352
        total:                           10816
    transactions:                        676    (67.57 per sec.)
    queries:                             10816  (1081.14 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          10.0030s
    total number of events:              676

Latency (ms):
         min:                                   12.26
         avg:                                   14.79
         max:                                   26.08
         95th percentile:                       17.01
         sum:                                10000.87

Threads fairness:
    events (avg/stddev):           676.0000/0.00
    execution time (avg/stddev):   10.0009/0.00


sysbench 1.0.20 (using bundled LuaJIT 2.1.0-beta2)
`

// TODO replace number of threads/seed
var SysbenchOutputTemplate = `
Running the test with following options:
Number of threads: 1
Initializing random number generator from seed (1).


Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            %d
        write:                           %d
        other:                           %d
        total:                           %d
    transactions:                        %d    (%.2f per sec.)
    queries:                             %d    (%.2f per sec.)
    ignored errors:                      %d    (%.2f per sec.)
    reconnects:                          %d    (%.2f per sec.)

General statistics:
    total time:                          %.2fs
    total number of events:              %d

Latency (ms):
         min:                                   %.2f
         avg:                                   %.2f
         max:                                   %.2f
         95th percentile:                       %.2f
         sum:                                   %.2f

Threads fairness:
    events (avg/stddev):           676.0000/0.00
    execution time (avg/stddev):   10.0009/0.00


sysbench 1.0.20 (using bundled LuaJIT 2.1.0-beta2)
`

// fromResultSysbenchOutput returns sysbench output based on the given Result
func fromResultSysbenchOutput(r *Result) string {
	return fmt.Sprintf(SysbenchOutputTemplate,
		r.SqlReadQueries,
		r.SqlWriteQueries,
		r.SqlOtherQueries,
		r.SqlTotalQueries,
		r.TransactionsTotal,
		r.TransactionsPerSecond,
		r.SqlTotalQueries,
		r.SqlTotalQueriesPerSecond,
		r.IgnoredErrorsTotal,
		r.IgnoredErrorsPerSecond,
		r.ReconnectsTotal,
		r.ReconnectsPerSecond,
		r.TotalTimeSeconds,
		r.TotalNumberOfEvents,
		r.LatencyMinMS,
		r.LatencyAvgMS,
		r.LatencyMaxMS,
		r.LatencyPercentile,
		r.LatencySumMS)
}

func TestFromOutputResults(t *testing.T) {
	tests := []struct {
		description    string
		output         []byte
		config         *Config
		serverConfig   *ServerConfig
		test           *Test
		expectedResult *Result
		expectedError  error
	}{
		{
			description: "should parse output into result",
			output:      []byte(sampleOutput1),
			config: &Config{
				RuntimeOS:     testOS,
				RuntimeGoArch: testGoArch,
			},
			serverConfig: &ServerConfig{
				Host:       "localhost",
				Server:     ServerType(testServer),
				Version:    testServerVersion,
				ServerExec: "test-exec",
			},
			test: &Test{
				Name:   testTestName,
				Params: []string{testTestParams},
			},
			expectedResult: &Result{
				Id:                       testId,
				SuiteId:                  testSuiteId,
				RuntimeOS:                testOS,
				RuntimeGoArch:            testGoArch,
				ServerName:               testServer,
				ServerVersion:            testServerVersion,
				TestName:                 testTestName,
				TestParams:               testTestParams,
				SqlReadQueries:           9464,
				SqlWriteQueries:          0,
				SqlOtherQueries:          1352,
				SqlTotalQueries:          10816,
				SqlTotalQueriesPerSecond: 1081.14,
				TransactionsTotal:        676,
				TransactionsPerSecond:    67.57,
				TotalTimeSeconds:         10.0030,
				TotalNumberOfEvents:      676,
				LatencyMinMS:             12.26,
				LatencyAvgMS:             14.79,
				LatencyMaxMS:             26.08,
				LatencyPercentile:        17.01,
				LatencySumMS:             10000.87,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			actual, err := FromOutputResult(test.output, test.config, test.serverConfig, test.test, testSuiteId, func() string {
				return testId
			})
			assert.Equal(t, test.expectedError, err)
			assert.Equal(t, test.expectedResult, actual)
		})
	}
}
