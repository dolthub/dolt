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

package val

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// ValueStore is an interface for a key-value store that can store byte sequences, keyed by a content hash.
// The only implementation is tree.NodeStore, but ValueStore can be used without depending on the tree package.
// This is useful for type handlers.
type ValueStore interface {
	ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error)
	WriteBytes(ctx context.Context, val []byte) (hash.Hash, error)
}
