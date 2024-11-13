// Copyright 2024 Dolthub, Inc.
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

package prolly

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// MutableMapFlusher provides methods for flushing the edits in a MutableMap, producing a new static MapInterface
// containing the edits.
type MutableMapFlusher[MapType MapInterface, TreeMap tree.MapInterface[val.Tuple, val.Tuple, val.TupleDesc]] interface {
	ApplyMutations(ctx context.Context, m *GenericMutableMap[MapType, TreeMap]) (TreeMap, error)

	ApplyMutationsWithSerializer(
		ctx context.Context,
		serializer message.Serializer,
		m *GenericMutableMap[MapType, TreeMap],
	) (TreeMap, error)

	Map(ctx context.Context, m *GenericMutableMap[MapType, TreeMap]) (MapType, error)
}
