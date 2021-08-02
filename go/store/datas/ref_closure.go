// Copyright 2019 Dolthub, Inc.
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

// RefClosure is a transitive closure of types.Ref.
type RefClosure interface {
	// Contains returns true if |ref| is contained in the closure.
	Contains(ctx context.Context, ref types.Ref) (bool, error)
}

// NewSetRefClosure computes the entire transitive closure of |ref|.
func NewSetRefClosure(ctx context.Context, vr types.ValueReader, ref types.Ref) (RefClosure, error) {
	s, err := transitiveClosure(ctx, vr, ref)
	if err != nil {
		return setRefClosure{}, err
	}

	return setRefClosure{HashSet: s}, nil
}

type setRefClosure struct {
	hash.HashSet
}

var _ RefClosure = setRefClosure{}

// Contains returns true if |ref| is contained in the closure.
func (s setRefClosure) Contains(ctx context.Context, ref types.Ref) (ok bool, err error) {
	ok = s.HashSet.Has(ref.TargetHash())
	return
}

func transitiveClosure(ctx context.Context, vr types.ValueReader, ref types.Ref) (s hash.HashSet, err error) {
	h := &RefByHeightHeap{ref}
	s = hash.NewHashSet()

	var curr types.RefSlice
	for !h.Empty() {
		curr = h.PopRefsOfHeight(h.MaxHeight())
		for _, r := range curr {
			s.Insert(r.TargetHash())
		}

		err = parentsToQueue(ctx, curr, h, vr)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// NewLazyRefClosure makes a lazy RefClosure, which computes the
// transitive closure of |ref| on demand to answer Contains() queries.
func NewLazyRefClosure(ref types.Ref, vr types.ValueReader) RefClosure {
	return lazyRefClosure{
		seen: hash.NewHashSet(ref.TargetHash()),
		heap: &RefByHeightHeap{ref},
		vr:   vr,
	}
}

type lazyRefClosure struct {
	seen hash.HashSet
	heap *RefByHeightHeap
	vr   types.ValueReader
}

var _ RefClosure = lazyRefClosure{}

// Contains returns true if |ref| is contained in the closure.
func (l lazyRefClosure) Contains(ctx context.Context, ref types.Ref) (ok bool, err error) {
	err = l.traverseBelowDepth(ctx, ref.Height())
	if err != nil {
		return false, err
	}
	return l.seen.Has(ref.TargetHash()), nil
}

// traverseBelowDepth traverses through all of the refs of height |depth| or higher,
// adding them to the set |l.seen|.
func (l lazyRefClosure) traverseBelowDepth(ctx context.Context, depth uint64) (err error) {
	var curr types.RefSlice
	for !l.heap.Empty() && depth <= l.heap.MaxHeight() {

		curr = l.heap.PopRefsOfHeight(l.heap.MaxHeight())
		for _, r := range curr {
			l.seen.Insert(r.TargetHash())
		}

		err = parentsToQueue(ctx, curr, l.heap, l.vr)
		if err != nil {
			return err
		}
	}
	return nil
}
