/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

// maxUint64 returns the maximum of two uint64
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// minUint64 returns the minimum of two uint64
func minUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}
