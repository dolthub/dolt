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
