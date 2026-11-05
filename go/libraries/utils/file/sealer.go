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

package file

import (
	"context"
	"os"
)

// Sealer seals the contents of a file at rest, and opens them. Dolt stores them as they are; see PlainSealer.
type Sealer interface {
	// Seal returns |contents| sealed. It must not modify |contents|.
	Seal(ctx context.Context, contents []byte) ([]byte, error)
	// Open returns the contents of the file at |path|, read as |data|. It may rewrite the file with |perm|.
	Open(ctx context.Context, path string, data []byte, perm os.FileMode) ([]byte, error)
}

// PlainSealer stores the contents of files as they are.
type PlainSealer struct{}

func (PlainSealer) Seal(_ context.Context, contents []byte) ([]byte, error) {
	return contents, nil
}

func (PlainSealer) Open(_ context.Context, _ string, data []byte, _ os.FileMode) ([]byte, error) {
	return data, nil
}

// SealerOf returns the first of |opts|, or a PlainSealer if there are none.
func SealerOf(opts []Sealer) Sealer {
	if len(opts) == 0 {
		return PlainSealer{}
	}
	return opts[0]
}
