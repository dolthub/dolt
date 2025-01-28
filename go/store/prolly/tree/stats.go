// Copyright 2023 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
)

type chunkDiff struct {
	from []hash.Hash
	to   []hash.Hash
}

// DiffChunksAtLevel returns a list of chunk diffs between two maps at a
// specific level.
func DiffChunksAtLevel[K, V ~[]byte, O Ordering[K]](ctx context.Context, level uint16, from, to StaticMap[K, V, O]) ([]chunkDiff, error) {
	if from.Root.level < level || to.Root.level < level {
		// |to| < level is a valid state, but  should have happened before calling
		return nil, fmt.Errorf("level %d invalid for from height: %d, %d", level, from.Root.level, to.Root.level)
	}
	fromNode := from.Root
	var err error
	for fromNode.level > level {
		fromNode, err = fetchChild(ctx, from.NodeStore, fromNode.getAddress(0))
		if err != nil {
			return nil, err
		}
	}

	toNode := to.Root
	for toNode.level > level {
		toNode, err = fetchChild(ctx, to.NodeStore, toNode.getAddress(0))
		if err != nil {
			return nil, err
		}
	}

	var diffs []chunkDiff
	i := 0
	j := 0
	for i < fromNode.Count() && j < toNode.Count() {
		fromAddr := fromNode.getAddress(i)
		toAddr := toNode.getAddress(j)
		if toAddr == fromAddr {
			// same
			i++
			j++
			continue
		}

		f := fromNode.GetKey(i)
		t := toNode.GetKey(j)
		cmp := from.Order.Compare(K(f), K(t))
		if cmp == 0 {
			// replace from->to
			diffs = append(diffs, chunkDiff{from: []hash.Hash{fromAddr}, to: []hash.Hash{toAddr}})
			i++
			j++
			continue
		} else {
			startI := i
			startJ := j
			for fromAddr != toAddr && cmp != 0 {
				if cmp < 0 {
					i++
					fromAddr = fromNode.getAddress(i)
				} else {
					j++
					toAddr = toNode.getAddress(j)
				}
				f = fromNode.GetKey(i)
				t = toNode.GetKey(j)
				cmp = from.Order.Compare(K(f), K(t))
			}
			// either addrs equal, or keys synced
			var newChunkDiff chunkDiff
			for k := startI; k < i; k++ {
				newChunkDiff.from = append(newChunkDiff.from, fromNode.getAddress(k))
			}
			for k := startJ; k < j; k++ {
				newChunkDiff.to = append(newChunkDiff.to, toNode.getAddress(k))
			}
			diffs = append(diffs, newChunkDiff)
		}
	}

	if i == fromNode.Count() && j < toNode.Count() {
		return diffs, nil
	}

	var newChunkDiff chunkDiff
	for i < fromNode.Count() {
		// deleted nodes
		newChunkDiff.from = append(newChunkDiff.from, fromNode.getAddress(i))
		i++

	}
	for j < toNode.Count() {
		// added nodes
		newChunkDiff.to = append(newChunkDiff.to, toNode.getAddress(i))
		j++
	}
	diffs = append(diffs, newChunkDiff)
	return diffs, nil
}

func GetChunksAtLevel[K, V ~[]byte, O Ordering[K]](ctx context.Context, m StaticMap[K, V, O], level int) ([]hash.Hash, error) {
	n := m.Root
	var err error
	for n.Level() > level {
		n, err = fetchChild(ctx, m.NodeStore, n.getAddress(0))
		if err != nil {
			return nil, err
		}
	}

	// get chunks at this level
	var ret []hash.Hash
	i := 0
	for i < n.Count() {
		ret = append(ret, n.getAddress(i))
	}
	return ret, nil
}

// GetHistogramLevel returns the highest internal level of the tree that has
// more than |low| addresses.
func GetHistogramLevel[K, V ~[]byte, O Ordering[K]](ctx context.Context, m StaticMap[K, V, O], low int) ([]Node, error) {
	if cnt, err := m.Count(); err != nil {
		return nil, err
	} else if cnt == 0 {
		return nil, nil
	}
	currentLevel := []Node{m.Root}
	level := m.Root.Level()
	for len(currentLevel) < low && level > 0 {
		var nextLevel []Node
		for _, node := range currentLevel {
			for i := 0; i < node.Count(); i++ {
				child, err := fetchChild(ctx, m.NodeStore, node.getAddress(i))
				if err != nil {
					return nil, err
				}
				nextLevel = append(nextLevel, child)
			}
		}
		currentLevel = nextLevel
		level--
	}
	return currentLevel, nil
}
