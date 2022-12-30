// Copyright 2022 Dolthub, Inc.
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

package sysbench

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPopulateHistogram(t *testing.T) {

	tests := []struct {
		name string
		in   []byte
		exp  Result
	}{
		{
			name: "w/ histogram",
			in: []byte(`
sysbench 1.0.20 (using system LuaJIT 2.1.0-beta3)

Creating table 'sbtest1'...
Inserting 10000 records into 'sbtest1'
Creating a secondary index on 'sbtest1'...
sysbench 1.0.20 (using system LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 1
Initializing random number generator from seed (1).


Initializing worker threads...

Threads started!

Latency histogram (values are in milliseconds)
       value  ------------- distribution ------------- count
       1.319 |****                                     3
       1.343 |********                                 6
       1.367 |******************                       14
       1.392 |*********************                    17
       1.417 |*******************************          25
       1.443 |**************************************** 32
       1.469 |************************                 19
       1.496 |*******************                      15
       1.523 |******                                   5
       1.551 |******                                   5
       1.579 |********                                 6
       1.608 |********                                 6
       1.637 |******                                   5
       1.667 |***                                      2
       1.697 |*****                                    4
       1.791 |*                                        1
       1.891 |*                                        1
       2.106 |****                                     3
       2.184 |***                                      2
       2.223 |****                                     3
       2.264 |***                                      2
       2.305 |****                                     3
       2.347 |****                                     3
       2.389 |****                                     3
       2.433 |******                                   5
       2.477 |***                                      2
       2.522 |****                                     3
       2.568 |***                                      2
       2.662 |*                                        1
       2.760 |*                                        1
       3.130 |*                                        1

SQL statistics:
    queries performed:
        read:                            200
        write:                           0
        other:                           0
        total:                           200
    transactions:                        200    (609.48 per sec.)
    queries:                             200    (609.48 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          0.3272s
    total number of events:              200

Latency (ms):
         min:                                    1.32
         avg:                                    1.62
         max:                                    3.14
         95th percentile:                        2.43
         sum:                                  324.91

Threads fairness:
    events (avg/stddev):           200.0000/0.00
    execution time (avg/stddev):   0.3249/0.00

sysbench 1.0.20 (using system LuaJIT 2.1.0-beta3)

Dropping table 'sbtest1'...`),
			exp: Result{
				time:   .327,
				iters:  200,
				avg:    1.62,
				median: 1.496,
				stddev: .369,
				hist: &Hist{
					bins: []float64{1.319, 1.343, 1.367, 1.392, 1.417, 1.443, 1.469, 1.496, 1.523, 1.551, 1.579, 1.608, 1.637, 1.667, 1.697, 1.791, 1.891, 2.106, 2.184, 2.223, 2.264, 2.305, 2.347, 2.389, 2.433, 2.477, 2.522, 2.568, 2.662, 2.760, 3.130},
					cnts: []int{3, 6, 14, 17, 25, 32, 19, 15, 5, 5, 6, 6, 5, 2, 4, 1, 1, 3, 2, 3, 2, 3, 3, 3, 5, 2, 3, 2, 1, 1, 1},
					cnt:  200,
					mn:   1.623,
					md:   1.496,
					v:    .136,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Result{}
			r.populateHistogram(tt.in)
			require.Equal(t, r.String(), tt.exp.String())
		})
	}
}

func TestLogJoin(t *testing.T) {
	t.Skip()
	scriptDir := "/Users/max-hoffman/go/src/github.com/dolthub/systab-sysbench-scripts"
	RunTestsFile(t, "testdata/log-join.yaml", scriptDir)
}
