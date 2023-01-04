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

package engine

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestSecondsSince(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	delta := secondsSince(start) * 1000
	expectedDelta := float64(time.Since(start) / time.Millisecond)
	diff := math.Abs(expectedDelta - delta)
	require.LessOrEqual(t, diff, float64(1/time.Millisecond))
}
