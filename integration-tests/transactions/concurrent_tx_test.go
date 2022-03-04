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

package transactions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcurrentTransactions(t *testing.T) {
	for _, test := range txTests {
		t.Run(test.name, func(t *testing.T) {
			testConcurrentTx(t, test)
		})
	}
}

type ConcurrentTxTest struct {
	name string
}

var txTests = []ConcurrentTxTest{
	{name: "todo"},
}

func testConcurrentTx(t *testing.T, test ConcurrentTxTest) {
	assert.True(t, true)
}
