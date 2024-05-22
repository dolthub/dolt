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

package tree

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

// AddrDiff is a pair of hash.Hash values representing the addresses of two nodes two different trees which are likely
// to be very similar.
type AddrDiff struct {
	From, To hash.Hash
}

type AddrDiffStream[K ~[]byte, O Ordering[K]] struct {
	fromNs NodeStore
	toNs   NodeStore
	from   Node
	to     Node
	dfr    []Differ[K, O]
}

type AddrDiffFn func(context.Context, AddrDiff) error

func (ads *AddrDiffStream[K, O]) Next(ctx context.Context) (AddrDiff, error) {
	for {
		if len(ads.dfr) > 0 {
			difr := ads.dfr[len(ads.dfr)-1]
			dif, err := difr.Next(ctx)
			if err != nil {
				if err != io.EOF {
					return AddrDiff{}, err
				}
				ads.dfr = ads.dfr[:len(ads.dfr)-1]
				continue
			}

			if dif.Type == ModifiedDiff {
				return AddrDiff{hash.Hash(dif.From), hash.Hash(dif.To)}, nil
			} // else, continue
		} else {
			return AddrDiff{}, io.EOF
		}
	}
}

var ErrRootDepthMismatch = errors.New("root depth mismatch")
var ErrShallowTree = errors.New("tree too shallow") // NM4 We can do better than this. TBD.
var ErrInvalidDepth = errors.New("invalid depth. must be > 0")

// layerDifferFromRoots returns a Differ that will compare the trees rooted at |from| and |to|, but for the purposes
// of comparing specific layers of the tree. If the trees are not the same depth, the Differ will return an error.
func layerDifferFromRoots[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromNs NodeStore, toNs NodeStore,
	from, to Node,
	order O,
) (AddrDiffStream[K, O], error) {
	// Currently we are punting on trees which aren't the same depth.
	if from.Level() != to.Level() {
		return AddrDiffStream[K, O]{}, ErrRootDepthMismatch
	}

	differs := make([]Differ[K, O], 0, from.Level())
	for i := from.Level(); i > 0; i-- {
		// We use the standard leaf diff walker, but adjust the cursors to the desired level.
		leafDiffer, err := DifferFromRoots[K, O](ctx, fromNs, toNs, from, to, order, false)
		if err != nil {
			return AddrDiffStream[K, O]{}, err
		}

		for leafDiffer.from.nd.Level() < i {
			leafDiffer.from = leafDiffer.from.parent
			leafDiffer.to = leafDiffer.to.parent
			leafDiffer.fromStop = leafDiffer.fromStop.parent
			leafDiffer.toStop = leafDiffer.toStop.parent
		}

		differs = append(differs, leafDiffer)
	}

	stream := AddrDiffStream[K, O]{
		fromNs: fromNs,
		toNs:   toNs,
		from:   from,
		to:     to,
		dfr:    differs,
	}
	return stream, nil

}

func ChunkAddressDiffOrderedTrees[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	from, to StaticMap[K, V, O],
	cb AddrDiffFn,
) error {
	differ, err := layerDifferFromRoots[K](ctx, from.NodeStore, to.NodeStore, from.Root, to.Root, from.Order)
	if err != nil {
		return err
	}

	for {
		var diff AddrDiff
		if diff, err = differ.Next(ctx); err != nil {
			break
		}

		if err = cb(ctx, diff); err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return err
}
