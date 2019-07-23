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

// MapStrings iterates over a slice of strings calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapStrings(strings []string, mapFunc func(string) string) []string {
	if strings == nil {
		return nil
	}

	results := make([]string, len(strings))

	for i, str := range strings {
		results[i] = mapFunc(str)
	}

	return results
}

// MapSlice iterates over a slice of calling the mapping function for each value in the
// slice.  The mapped values are returned in a new slice, and their order corresponds with
// the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapSlice(sl []interface{}, mapFunc func(interface{}) interface{}) []interface{} {
	if sl == nil {
		return nil
	}

	results := make([]interface{}, len(sl))
	for i, item := range sl {
		results[i] = mapFunc(item)
	}

	return results
}

// MapFloat64s iterates over a slice of float64s calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapFloat64s(floats []float64, mapFunc func(float64) float64) []float64 {
	if floats == nil {
		return nil
	}

	results := make([]float64, len(floats))

	for i, fl := range floats {
		results[i] = mapFunc(fl)
	}

	return results
}

// MapFloat32s iterates over a slice of floats calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapFloat32s(floats []float32, mapFunc func(float32) float32) []float32 {
	if floats == nil {
		return nil
	}

	results := make([]float32, len(floats))

	for i, fl := range floats {
		results[i] = mapFunc(fl)
	}

	return results
}

// MapInts iterates over a slice of ints calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapInts(ints []int, mapFunc func(int) int) []int {
	if ints == nil {
		return nil
	}

	results := make([]int, len(ints))

	for i, fl := range ints {
		results[i] = mapFunc(fl)
	}

	return results
}

// MapInt64s iterates over a slice of int64s calling the mapping function for each value
// in the slice.  The mapped values are returned in a new slice, and their order corresponds
// with the input slice (The Nth item in the output slice is the result returned by the mapping
// function when given the Nth item from the input slice.)
func MapInt64s(ints []int64, mapFunc func(int64) int64) []int64 {
	if ints == nil {
		return nil
	}

	results := make([]int64, len(ints))

	for i, fl := range ints {
		results[i] = mapFunc(fl)
	}

	return results
}
