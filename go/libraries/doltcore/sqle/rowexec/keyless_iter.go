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

package rowexec

import (
	"context"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func newKeylessMapIter(iter prolly.MapIter) prolly.MapIter {
	return &keylessMapIter{iter: iter}
}

// keylessMapIter duplicates keyless rows using the cardinality column
type keylessMapIter struct {
	iter prolly.MapIter
	card uint64
	key  val.Tuple
	val  val.Tuple
}

var _ prolly.MapIter = (*keylessMapIter)(nil)

func (k *keylessMapIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	var err error
	if k.key == nil {
		k.key, k.val, err = k.iter.Next(ctx)
		if err != nil {
			return nil, nil, err
		}
		k.card = val.ReadKeylessCardinality(k.val)
	}

	if k.card == 0 {
		k.key = nil
		k.val = nil
		return k.Next(ctx)
	}

	k.card--
	return k.key, k.val, nil
}
