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

package funcitr

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestMapStrings(t *testing.T) {
	inputs := []string{"this", "THAT", "The", "oThEr"}
	outputs := MapStrings(inputs, strings.ToLower)

	if !reflect.DeepEqual(outputs, []string{"this", "that", "the", "other"}) {
		t.Error("Failed to map over strings")
	}
}

func TestMapSlice(t *testing.T) {
	inputs := []interface{}{2, 4, 6, 8}
	outputs := MapSlice(inputs, func(item interface{}) interface{} {
		n := item.(int)
		return strconv.FormatInt(int64(n), 10)
	})

	if !reflect.DeepEqual(outputs, []interface{}{"2", "4", "6", "8"}) {
		t.Error("Failed to map over strings")
	}
}

func TestMapInts(t *testing.T) {
	inputs32 := []int{2, 4, 6, 8}
	inputs64 := []int64{2, 4, 6, 8}

	outputs32 := MapInts(inputs32, func(x int) int {
		return x * 2
	})
	outputs64 := MapInt64s(inputs64, func(x int64) int64 {
		return x * 2
	})

	if !reflect.DeepEqual(outputs32, []int{4, 8, 12, 16}) {
		t.Error("Failed to map over strings")
	}

	if !reflect.DeepEqual(outputs64, []int64{4, 8, 12, 16}) {
		t.Error("Failed to map over strings")
	}

}

func TestMapFloats(t *testing.T) {
	inputs32 := []float32{2, 4, 6, 8}
	inputs64 := []float64{2, 4, 6, 8}

	outputs32 := MapFloat32s(inputs32, func(x float32) float32 {
		return x * 2
	})
	outputs64 := MapFloat64s(inputs64, func(x float64) float64 {
		return x * 2
	})

	if !reflect.DeepEqual(outputs32, []float32{4, 8, 12, 16}) {
		t.Error("Failed to map over strings")
	}

	if !reflect.DeepEqual(outputs64, []float64{4, 8, 12, 16}) {
		t.Error("Failed to map over strings")
	}
}

func TestNilSlices(t *testing.T) {
	str := MapStrings(nil, func(s string) string {
		return ""
	})
	f32 := MapFloat32s(nil, func(f float32) float32 {
		return 0.0
	})
	f64 := MapFloat64s(nil, func(f float64) float64 {
		return 0.0
	})
	i := MapInts(nil, func(x int) int {
		return 0
	})
	i64 := MapInt64s(nil, func(i int64) int64 {
		return 0
	})
	sl := MapSlice(nil, func(item interface{}) interface{} {
		return nil
	})

	if str != nil || f32 != nil || f64 != nil || i != nil || i64 != nil || sl != nil {
		t.Error("bad mapping of nil input slice")
	}
}
