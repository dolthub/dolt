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

package chunks

import (
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
)

var ErrMissingChunk = errors.New("missing chunk")

// MissingObjectError implements the error interface and holds dynamic data regarding which object was missing.
type MissingChunkError struct {
	h hash.Hash
}

func (e *MissingChunkError) Error() string {
	return fmt.Sprintf("malformed database: chunk %s required but not found", e.h.String())
}

// Unwrap ensures that errors.Is(err, ErrMissingObject) works correctly
// by pointing back to the package-level sentinel error.
func (e *MissingChunkError) Unwrap() error {
	return ErrMissingChunk
}

// NewMissingObjectError is a constructor function for convenience.
func NewMissingChunkError(h hash.Hash) *MissingChunkError {
	return &MissingChunkError{
		h: h,
	}
}
