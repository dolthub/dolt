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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"container/heap"
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ParentsField = "parents"
	// Added in July, 2020. Commits created with versions before this was
	// added have only a Set of parents. Commits created after this was
	// added carry a List of parents, because parent order can matter.
	// `"parents"` is still written as a Set as well, so that commits
	// created with newer versions of still usable by older versions.
	ParentsListField = "parents_list"
	// Added in October, 2021. Stores a Ref<Value(Map<Tuple,List>)>.
	// The key of the map is a Tuple<Height, InlineBlob(Hash)>, reffable to
	// a Commit ref. The value of the map is a List of Ref<Value>s pointing
	// to the parents of the Commit which corresponds to the key.
	//
	// This structure is a materialized closure of a commit's parents. It
	// is used for pull/fetch/push commit graph fan-out and for sub-O(n)
	// FindCommonAncestor calculations.
	ParentsClosureField = "parents_closure"
	ValueField          = "value"
	CommitMetaField     = "meta"
	CommitName          = "Commit"
)

var commitTemplateWithParentsClosure = types.MakeStructTemplate(CommitName, []string{
	CommitMetaField,
	ParentsField,
	ParentsClosureField,
	ParentsListField,
	ValueField,
})

var commitTemplateWithoutParentsClosure = types.MakeStructTemplate(CommitName, []string{
	CommitMetaField,
	ParentsField,
	ParentsListField,
	ValueField,
})

var valueCommitType = nomdl.MustParseType(`Struct Commit {
        meta: Struct {},
        parents: Set<Ref<Cycle<Commit>>>,
        parents_closure?: Ref<Value>, // Ref<Map<Value,Value>>,
        parents_list?: List<Ref<Cycle<Commit>>>,
        value: Value,
}`)

// newCommit creates a new commit object.
//
// A commit has the following type:
//
// ```
// struct Commit {
//   meta: M,
//   parents: Set<Ref<Cycle<Commit>>>,
//   parentsList: List<Ref<Cycle<Commit>>>,
//   parentsClosure: Ref<Value>, // Map<Tuple,List<Ref<Value>>>,
//   value: T,
// }
// ```
// where M is a struct type and T is any type.
func newCommit(ctx context.Context, value types.Value, parentsList types.List, parentsClosure types.Ref, includeParentsClosure bool, meta types.Struct) (types.Struct, error) {
	parentsSet, err := parentsList.ToSet(ctx)
	if err != nil {
		return types.EmptyStruct(meta.Format()), err
	}
	if includeParentsClosure {
		return commitTemplateWithParentsClosure.NewStruct(meta.Format(), []types.Value{meta, parentsSet, parentsClosure, parentsList, value})
	} else {
		return commitTemplateWithoutParentsClosure.NewStruct(meta.Format(), []types.Value{meta, parentsSet, parentsList, value})
	}
}

func NewCommitForValue(ctx context.Context, vrw types.ValueReadWriter, v types.Value, opts CommitOptions) (types.Struct, error) {
	if opts.ParentsList == types.EmptyList || opts.ParentsList.Len() == 0 {
		return types.Struct{}, errors.New("cannot create commit without parents")
	}

	if opts.Meta.IsZeroValue() {
		opts.Meta = types.EmptyStruct(vrw.Format())
	}

	parentsClosure, includeParentsClosure, err := getParentsClosure(ctx, vrw, opts.ParentsList)
	if err != nil {
		return types.Struct{}, err
	}

	return newCommit(ctx, v, opts.ParentsList, parentsClosure, includeParentsClosure, opts.Meta)
}

func FindCommonAncestorUsingParentsList(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (types.Ref, bool, error) {
	c1Q, c2Q := RefByHeightHeap{c1}, RefByHeightHeap{c2}
	for !c1Q.Empty() && !c2Q.Empty() {
		c1Ht, c2Ht := c1Q.MaxHeight(), c2Q.MaxHeight()
		if c1Ht == c2Ht {
			c1Parents, c2Parents := c1Q.PopRefsOfHeight(c1Ht), c2Q.PopRefsOfHeight(c2Ht)
			if common, ok := findCommonRef(c1Parents, c2Parents); ok {
				return common, true, nil
			}
			err := parentsToQueue(ctx, c1Parents, &c1Q, vr1)
			if err != nil {
				return types.Ref{}, false, err
			}
			err = parentsToQueue(ctx, c2Parents, &c2Q, vr2)
			if err != nil {
				return types.Ref{}, false, err
			}
		} else if c1Ht > c2Ht {
			err := parentsToQueue(ctx, c1Q.PopRefsOfHeight(c1Ht), &c1Q, vr1)
			if err != nil {
				return types.Ref{}, false, err
			}
		} else {
			err := parentsToQueue(ctx, c2Q.PopRefsOfHeight(c2Ht), &c2Q, vr2)
			if err != nil {
				return types.Ref{}, false, err
			}
		}
	}

	return types.Ref{}, false, nil
}

// FindCommonAncestor returns the most recent common ancestor of c1 and c2, if
// one exists, setting ok to true. If there is no common ancestor, ok is set
// to false. Refs of |c1| are dereferenced through |vr1|, while refs of |c2|
// are dereference through |vr2|.
//
// This implementation makes use of the parents_closure field on the commit
// struct.  If the commit does not have a materialized parents_closure, this
// implementation delegates to FindCommonAncestorUsingParentsList.
func FindCommonAncestor(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (types.Ref, bool, error) {
	pi1, err := newParentsClosureIterator(ctx, c1, vr1)
	if err != nil {
		return types.Ref{}, false, err
	}
	if pi1 == nil {
		return FindCommonAncestorUsingParentsList(ctx, c1, c2, vr1, vr2)
	}

	pi2, err := newParentsClosureIterator(ctx, c2, vr2)
	if err != nil {
		return types.Ref{}, false, err
	}
	if pi2 == nil {
		return FindCommonAncestorUsingParentsList(ctx, c1, c2, vr1, vr2)
	}

	for {
		h1, h2 := pi1.Hash(), pi2.Hash()
		if h1 == h2 {
			if err := firstError(pi1.Err(), pi2.Err()); err != nil {
				return types.Ref{}, false, err
			}
			r, err := hashToRef(ctx, vr1, h1)
			if err != nil {
				return types.Ref{}, false, err
			}
			return r, true, nil
		}
		if pi1.Less(vr1.Format(), pi2) {
			// TODO: Should pi2.Seek(pi1.curr), but MapIterator does not expose Seek yet.
			if !pi2.Next(ctx) {
				return types.Ref{}, false, firstError(pi1.Err(), pi2.Err())
			}
		} else {
			if !pi1.Next(ctx) {
				return types.Ref{}, false, firstError(pi1.Err(), pi2.Err())
			}
		}
	}
}

// FindClosureCommonAncestor returns the most recent common ancestor of |cl| and |cm|,
// where |cl| is the transitive closure of one or more refs. If a common ancestor
// exists, |ok| is set to true, else false.
func FindClosureCommonAncestor(ctx context.Context, cl RefClosure, cm types.Ref, vr types.ValueReader) (a types.Ref, ok bool, err error) {
	q := &RefByHeightHeap{cm}
	var curr types.RefSlice

	for !q.Empty() {
		curr = q.PopRefsOfHeight(q.MaxHeight())

		for _, r := range curr {
			ok, err = cl.Contains(ctx, r)
			if err != nil {
				return types.Ref{}, false, err
			}
			if ok {
				return r, ok, nil
			}
		}

		err = parentsToQueue(ctx, curr, q, vr)
		if err != nil {
			return types.Ref{}, false, err
		}
	}

	return types.Ref{}, false, nil
}

func parentsToQueue(ctx context.Context, refs types.RefSlice, q *RefByHeightHeap, vr types.ValueReader) error {
	seen := make(map[hash.Hash]bool)
	for _, r := range refs {
		if _, ok := seen[r.TargetHash()]; ok {
			continue
		}
		seen[r.TargetHash()] = true

		v, err := r.TargetValue(ctx, vr)
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("target not found: %v", r.TargetHash())
		}

		c, ok := v.(types.Struct)
		if !ok {
			return fmt.Errorf("target ref is not struct: %v", v)
		}
		if c.Name() != CommitName {
			return fmt.Errorf("target ref is not commit: %v", v)
		}
		ps, ok, err := c.MaybeGet(ParentsListField)
		if err != nil {
			return err
		}
		if ok {
			p := ps.(types.List)
			err = p.Iter(ctx, func(v types.Value, _ uint64) (stop bool, err error) {
				heap.Push(q, v)
				return
			})
			if err != nil {
				return err
			}
		} else {
			ps, ok, err := c.MaybeGet(ParentsField)
			if err != nil {
				return err
			}
			if ok {
				p := ps.(types.Set)
				err = p.Iter(ctx, func(v types.Value) (stop bool, err error) {
					heap.Push(q, v)
					return
				})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func findCommonRef(a, b types.RefSlice) (types.Ref, bool) {
	toRefSet := func(s types.RefSlice) map[hash.Hash]types.Ref {
		out := map[hash.Hash]types.Ref{}
		for _, r := range s {
			out[r.TargetHash()] = r
		}
		return out
	}

	aSet, bSet := toRefSet(a), toRefSet(b)
	for s, r := range aSet {
		if _, present := bSet[s]; present {
			return r, true
		}
	}
	return types.Ref{}, false
}

func makeCommitStructType(metaType, parentsType, parentsListType, parentsClosureType, valueType *types.Type, includeParentsClosure bool) (*types.Type, error) {
	if includeParentsClosure {
		return types.MakeStructType(CommitName,
			types.StructField{
				Name: CommitMetaField,
				Type: metaType,
			},
			types.StructField{
				Name: ParentsField,
				Type: parentsType,
			},
			types.StructField{
				Name: ParentsListField,
				Type: parentsListType,
			},
			types.StructField{
				Name: ParentsClosureField,
				Type: parentsClosureType,
			},
			types.StructField{
				Name: ValueField,
				Type: valueType,
			},
		)
	} else {
		return types.MakeStructType(CommitName,
			types.StructField{
				Name: CommitMetaField,
				Type: metaType,
			},
			types.StructField{
				Name: ParentsField,
				Type: parentsType,
			},
			types.StructField{
				Name: ParentsListField,
				Type: parentsListType,
			},
			types.StructField{
				Name: ValueField,
				Type: valueType,
			},
		)
	}
}

func getRefElementType(t *types.Type) *types.Type {
	// precondition checks
	d.PanicIfFalse(t.TargetKind() == types.RefKind)

	return t.Desc.(types.CompoundDesc).ElemTypes[0]
}

type parentsClosureIterator struct {
	mi   types.MapIterator
	err  error
	curr types.Tuple
}

func (i *parentsClosureIterator) Err() error {
	return i.err
}

func (i *parentsClosureIterator) Hash() hash.Hash {
	if i.err != nil {
		return hash.Hash{}
	}
	var h hash.Hash
	field, err := i.curr.Get(1)
	if err != nil {
		i.err = err
		return hash.Hash{}
	}
	ib, ok := field.(types.InlineBlob)
	if !ok {
		i.err = fmt.Errorf("second field of tuple key parents closure should have been InlineBlob")
		return hash.Hash{}
	}
	copy(h[:], []byte(ib))
	return h
}

func (i *parentsClosureIterator) Less(f *types.NomsBinFormat, other *parentsClosureIterator) bool {
	if i.err != nil || other.err != nil {
		return false
	}
	ret, err := i.curr.Less(f, other.curr)
	if err != nil {
		i.err = err
		other.err = err
		return false
	}
	return ret
}

func (i *parentsClosureIterator) Next(ctx context.Context) bool {
	if i.err != nil {
		return false
	}
	n, _, err := i.mi.Next(ctx)
	if err != nil {
		i.err = err
		return false
	}
	if n == nil || types.IsNull(n) {
		return false
	}
	t, ok := n.(types.Tuple)
	if !ok {
		i.err = fmt.Errorf("key value of parents closure map should have been Tuple")
		return false
	}
	i.curr = t
	return true
}

func hashToRef(ctx context.Context, vr types.ValueReader, h hash.Hash) (types.Ref, error) {
	fetched, err := vr.ReadValue(ctx, h)
	if err != nil {
		return types.Ref{}, err
	}
	return types.NewRef(fetched, vr.Format())
}

func refToMapKeyTuple(f *types.NomsBinFormat, r types.Ref) (types.Tuple, error) {
	h := r.TargetHash()
	ib := make([]byte, len(hash.Hash{}))
	copy(ib, h[:])
	return types.NewTuple(f, types.Uint(r.Height()), types.InlineBlob(ib))
}

func firstError(l, r error) error {
	if l != nil {
		return l
	}
	return r
}

func newParentsClosureIterator(ctx context.Context, r types.Ref, vr types.ValueReader) (*parentsClosureIterator, error) {
	sv, err := r.TargetValue(ctx, vr)
	if err != nil {
		return nil, err
	}
	s, ok := sv.(types.Struct)
	if !ok {
		return nil, fmt.Errorf("target ref is not struct: %v", sv)
	}
	if s.Name() != CommitName {
		return nil, fmt.Errorf("target ref is not commit: %v", sv)
	}

	fv, ok, err := s.MaybeGet(ParentsClosureField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	mr, ok := fv.(types.Ref)
	if !ok {
		return nil, fmt.Errorf("value of parents_closure field is not Ref: %v", fv)
	}

	mv, err := mr.TargetValue(ctx, vr)
	if err != nil {
		return nil, err
	}

	m, ok := mv.(types.Map)
	if !ok {
		return nil, fmt.Errorf("target value of parents_closure Ref is not Map: %v", mv)
	}

	maxKeyTuple, err := types.NewTuple(vr.Format(), types.Uint(18446744073709551615))
	if err != nil {
		return nil, err
	}

	mi, err := m.IteratorBackFrom(ctx, maxKeyTuple)
	if err != nil {
		return nil, err
	}

	initialCurr, err := refToMapKeyTuple(vr.Format(), r)
	if err != nil {
		return nil, err
	}

	return &parentsClosureIterator{mi, nil, initialCurr}, nil
}

func IsCommitType(nbf *types.NomsBinFormat, t *types.Type) bool {
	return types.IsSubtype(nbf, valueCommitType, t)
}

func IsCommit(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		return types.IsValueSubtypeOf(s.Format(), v, valueCommitType)
	}
}

func IsRefOfCommitType(nbf *types.NomsBinFormat, t *types.Type) bool {
	return t.TargetKind() == types.RefKind && IsCommitType(nbf, getRefElementType(t))
}

type RefByHeightHeap []types.Ref

func (r RefByHeightHeap) Less(i, j int) bool {
	return types.HeightOrder(r[i], r[j])
}

func (r RefByHeightHeap) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RefByHeightHeap) Len() int {
	return len(r)
}

func (r *RefByHeightHeap) Push(x interface{}) {
	*r = append(*r, x.(types.Ref))
}

func (r *RefByHeightHeap) Pop() interface{} {
	old := *r
	ret := old[len(old)-1]
	*r = old[:len(old)-1]
	return ret
}

func (r RefByHeightHeap) Empty() bool {
	return len(r) == 0
}

func (r RefByHeightHeap) MaxHeight() uint64 {
	return r[0].Height()
}

func (r *RefByHeightHeap) PopRefsOfHeight(h uint64) types.RefSlice {
	var ret types.RefSlice
	for !r.Empty() && r.MaxHeight() == h {
		ret = append(ret, heap.Pop(r).(types.Ref))
	}
	return ret
}
