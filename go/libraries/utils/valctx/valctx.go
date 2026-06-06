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

package valctx

import (
	"context"
)

var enabled bool

// Globally enables context validation for the process. If this is not
// called, then the other functions in this package are noops.
func EnableContextValidation() {
	enabled = true
}

// SetEnabledForTest sets the enabled flag and returns its previous value, so a
// test can flip validation on for the duration of a single test and restore
// the prior global state on cleanup. Production code should use
// EnableContextValidation; SetEnabledForTest is intended for tests that need
// to observe (or avoid) valctx behavior without leaking state to siblings.
func SetEnabledForTest(v bool) bool {
	prev := enabled
	enabled = v
	return prev
}

func IsEnabled() bool {
	return enabled
}

type ctxKey int

var validationKey ctxKey

func WithContextValidation(ctx context.Context) context.Context {
	if !enabled {
		return ctx
	}
	return context.WithValue(ctx, validationKey, new(Validation))
}

type Validation func()

func SetContextValidation(ctx context.Context, validation Validation) {
	if !enabled {
		return
	}
	*ctx.Value(validationKey).(*Validation) = validation
}

func ValidateContext(ctx context.Context) {
	if !enabled {
		return
	}
	(*ctx.Value(validationKey).(*Validation))()
}
