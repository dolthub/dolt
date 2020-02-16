// Copyright 2020 Liquidata, Inc.
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

package typeinfo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/utils/mathutil"
)

func loop(t *testing.T, start int64, endInclusive int64, numOfSteps uint16, loopedFunc func(int64)) {
	require.True(t, endInclusive > start)
	maxNumOfSteps := endInclusive - start + 1
	trueNumOfSteps := mathutil.MinInt64(int64(numOfSteps), maxNumOfSteps) - 1
	inc := float64(maxNumOfSteps) / float64(trueNumOfSteps)
	fCurrentVal := float64(start)
	currentVal := int64(math.Round(fCurrentVal))
	fCurrentVal -= 1
	for currentVal <= endInclusive {
		loopedFunc(currentVal)
		fCurrentVal += inc
		currentVal = int64(math.Round(fCurrentVal))
	}
}
