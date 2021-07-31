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

// todo comment doc
type RefClosure interface {
	Contains(ref types.Ref) (bool, error)
}

// todo comment doc
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

func (s setRefClosure) Contains(ref types.Ref) (ok bool, err error) {
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

func NewLazyRefClousure(ctx context.Context, ref types.Ref, vr types.ValueReader) lazyRefClosure {
	return lazyRefClosure{
		partial: hash.NewHashSet(ref.TargetHash()),
		bottom:  RefByHeightHeap{ref},
	}
}

type lazyRefClosure struct {
	partial hash.HashSet
	bottom  []types.Ref
	depth   uint64
}

var _ RefClosure = lazyRefClosure{}

func (l lazyRefClosure) Contains(ref types.Ref) (ok bool, err error) {
	if ref.Height() < l.depth {
		err = traverseToDepth(ref.Height(), l.bottom, l.partial)
	}
	if err != nil {
		return false, err
	}
	return l.partial.Has(ref.TargetHash()), nil
}

func traverseToDepth(depth uint64, roots []types.Ref, visited hash.HashSet) error {
	panic("todo")
}
