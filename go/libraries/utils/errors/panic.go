// Copyright 2026 Dolthub, Inc.
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

package errors

import (
	"fmt"
)

type FatalBehavior int

const (
	// Returns an error on a fatal error.
	FatalBehaviorError FatalBehavior = iota

	// Crashes the process immediately and without returning on a fatal error.
	FatalBehaviorCrash
)

// Fatalf signals a fatal error, and can be used in situations where the process may
// be entering an unsafe state due to the encountered error. If |behavior| is
// FatalBehaviorCrash, this function will never return. Otherwise, an error value is
// returned, built with fmt.Errorf on |msg| and |args|.
func Fatalf(behavior FatalBehavior, msg string, args ...any) error {
	if behavior == FatalBehaviorCrash {
		go func() {
			panic(fmt.Sprintf("fatal error: "+msg, args...))
		}()
		for {
		}
	} else {
		return fmt.Errorf("fatal error: "+msg, args...)
	}
}
