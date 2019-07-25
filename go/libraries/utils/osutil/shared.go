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

package osutil

// StartsWithWindowsVolume checks if the given string begins with a valid Windows Volume e.g. "C:" or "Z:"
func StartsWithWindowsVolume(p string) bool {
	if len(p) >= 2 && p[0] >= 'A' && p[0] <= 'Z' && p[1] == ':' {
		return true
	}
	return false
}
