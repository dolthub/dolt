// Copyright 2025 Dolthub, Inc.
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

package dynassert

import (
	"fmt"
	"os"
)

// dynamic asserts are enabled by default so that they run during unit
// tests, for example.
var enabled bool = true

// To be used in the top-level |main|, this disables dynamic asserts
// unless a specific environment variable is set.
func InitDyanmicAsserts() {
	if os.Getenv("DOLT_ENABLE_DYNAMIC_ASSERTS") == "" {
		enabled = false
	}
}

// Dynamically enabled assertions. These are software integrity sanity
// checks that are enabled for tests, both unit and integration, and
// can be enabled anytime when we are running in a controlled
// environment where we want to fail hard if they are violated.
// Typically these are not enabled when `dolt` is running for its
// users. Code making use fo dynasserts should recover gracefully even
// when they fail.

// If dynasserts are enabled and cond is false, panics with the
// formatted string Sprintf(msg, args...).  Otherwise returns |cond|.
//
// A suggested usage might be something like:
//
//	if dynassert.Assert(atomic.AddInt32(refcnt, 1) <= 1, "invalid ref count; incremented from <= 0") {
//	    // Restore, since we are not taking the reference...
//	    atomic.AddInt32(refcnt, -1)
//	    return NewObjectInstead(...)
//	}
//
// return view_of_this_object_with_valid_ref
func Assert(cond bool, msg string, args ...any) bool {
	if enabled {
		if !cond {
			panic(fmt.Sprintf(msg, args...))
		}
	}
	return cond
}
