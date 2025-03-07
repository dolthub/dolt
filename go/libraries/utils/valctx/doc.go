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

// Package valctx provides an interface for pluggable Context
// validation in situations where a Context lifecycle might need to be
// sanity checked. If Context validation is enabled, then storing a
// Validation on a Context which has already gone through
// WithContextValidation will cause the Validation to be called from
// ValidateContext.
//
// For the time being, validations do not return anything. They can
// panic in the case of a critical error, or choose to asynchronously
// report failures.
package valctx
