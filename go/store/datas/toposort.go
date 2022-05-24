// Copyright 2022 Dolthub, Inc.
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

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type topoState struct {
	vr   types.ValueReader
	out  []*Commit
	seen map[hash.Hash]*Commit
}

// TopologicallySortCommits performs a post-DFS on [c]'s parent graph, returning a list
// of datas.Commit (or an error) with the most recent commit first and founding commit
// last. The traverse short circuits if [n] >= 0 and is less than the number of the
// commits in the graph.
func TopologicallySortCommits(ctx context.Context, vr types.ValueReader, c *Commit, n int) ([]*Commit, error) {
	s := &topoState{
		vr:   vr,
		out:  make([]*Commit, 0),
		seen: make(map[hash.Hash]*Commit),
	}
	err := topoSort(ctx, s, c, n)
	if err != nil {
		return nil, err
	}
	i := 0
	j := len(s.out) - 1
	for i < j {
		s.out[i], s.out[j] = s.out[j], s.out[i]
		i++
		j--
	}
	return s.out, nil
}

func topoSort(ctx context.Context, s *topoState, c *Commit, n int) error {
	parents, err := GetCommitParents(ctx, s.vr, c.NomsValue())
	if err != nil {
		return err
	}

	for _, p := range parents {
		if _, ok := s.seen[p.addr]; ok {
			continue
		}
		s.seen[p.addr] = p
		if n > 0 && len(s.seen) >= n {
			return nil
		}
		err = topoSort(ctx, s, p, n)
		if err != nil {
			return err
		}
	}

	s.out = append(s.out, c)
	return nil
}
