// Copyright 2019 Liquidata, Inc.
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

package mathutil

var Max = MaxInt // alias
var Min = MinInt // alias

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUint(a, b uint) uint {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint(a, b uint) uint {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxFloat(a, b float32) float32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinFloat(a, b float32) float32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxFloat64(a, b float64) float64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinFloat64(a, b float64) float64 {
	if a < b {
		return a
	} else {
		return b
	}
}
