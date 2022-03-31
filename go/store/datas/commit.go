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

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	parentsField = "parents"
	// Added in July, 2020. Commits created with versions before this was
	// added have only a Set of parents. Commits created after this was
	// added carry a List of parents, because parent order can matter.
	// `"parents"` is still written as a Set as well, so that commits
	// created with newer versions of still usable by older versions.
	parentsListField = "parents_list"
	// Added in October, 2021. Stores a Ref<Value(Map<Tuple,List>)>.
	// The key of the map is a Tuple<Height, InlineBlob(Hash)>, reffable to
	// a Commit ref. The value of the map is a List of Ref<Value>s pointing
	// to the parents of the Commit which corresponds to the key.
	//
	// This structure is a materialized closure of a commit's parents. It
	// is used for pull/fetch/push commit graph fan-out and for sub-O(n)
	// FindCommonAncestor calculations.
	parentsClosureField = "parents_closure"
	valueField          = "value"
	commitMetaField     = "meta"
	commitName          = "Commit"
)

type Commit struct {
	val    types.Value
	addr   hash.Hash
	height uint64
}

func (c *Commit) NomsValue() types.Value {
	return c.val
}

func (c *Commit) Height() uint64 {
	return c.height
}

func (c *Commit) Addr() hash.Hash {
	return c.addr
}

var commitTemplateWithParentsClosure = types.MakeStructTemplate(commitName, []string{
	commitMetaField,
	parentsField,
	parentsClosureField,
	parentsListField,
	valueField,
})

var commitTemplateWithoutParentsClosure = types.MakeStructTemplate(commitName, []string{
	commitMetaField,
	parentsField,
	parentsListField,
	valueField,
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

func NewCommitForValue(ctx context.Context, vrw types.ValueReadWriter, v types.Value, opts CommitOptions) (*Commit, error) {
	if opts.Parents == nil || len(opts.Parents) == 0 {
		return nil, errors.New("cannot create commit without parents")
	}

	return newCommitForValue(ctx, vrw, v, opts)
}

func commit_flatbuffer(vaddr hash.Hash, opts CommitOptions, heights []uint64) ([]byte, uint64) {
	builder := flatbuffers.NewBuilder(1024)
	vaddroff := builder.CreateByteVector(vaddr[:])

	hashsz := 20
	hashessz := len(opts.Parents) * hashsz
	builder.Prep(flatbuffers.SizeUOffsetT, hashessz)
	stop := int(builder.Head())
	start := stop - hashessz
	for i := 0; i < len(opts.Parents); i++ {
		copy(builder.Bytes[start:stop], opts.Parents[i][:])
		start += hashsz
	}
	start = stop - hashessz
	parentaddrsoff := builder.CreateByteVector(builder.Bytes[start:stop])

	// Starts at SerialMessageRefHeight.
	maxheight := uint64(types.SerialMessageRefHeight)
	serial.CommitStartParentHeightsVector(builder, len(opts.Parents))
	for i := len(opts.Parents) - 1; i >= 0; i-- {
		builder.PrependUint64(heights[i])
		if heights[i] > maxheight {
			maxheight = heights[i]
		}
	}
	parentheightsoff := builder.EndVector(len(opts.Parents))

	nameoff := builder.CreateString(opts.Meta.Name)
	emailoff := builder.CreateString(opts.Meta.Email)
	descoff := builder.CreateString(opts.Meta.Description)
	serial.CommitStart(builder)
	serial.CommitAddRoot(builder, vaddroff)
	serial.CommitAddHeight(builder, maxheight+1)
	serial.CommitAddParentAddrs(builder, parentaddrsoff)
	serial.CommitAddParentHeights(builder, parentheightsoff)
	serial.CommitAddName(builder, nameoff)
	serial.CommitAddEmail(builder, emailoff)
	serial.CommitAddDescription(builder, descoff)
	serial.CommitAddTimestampMillis(builder, opts.Meta.Timestamp)
	serial.CommitAddUserTimestampMillis(builder, opts.Meta.UserTimestamp)
	builder.FinishWithFileIdentifier(serial.CommitEnd(builder), []byte(serial.CommitFileID))
	return builder.FinishedBytes(), maxheight+1
}

func newCommitForValue(ctx context.Context, vrw types.ValueReadWriter, v types.Value, opts CommitOptions) (*Commit, error) {
	if opts.Meta == nil {
		opts.Meta = &CommitMeta{}
	}

	if vrw.Format() == types.Format_DOLT_DEV {
		r, err := vrw.WriteValue(ctx, v)
		if err != nil {
			return nil, err
		}
		heights := make([]uint64, len(opts.Parents))
		parents, err := vrw.ReadManyValues(ctx, opts.Parents)
		if err != nil {
			return nil, err
		}
		for i := range heights {
			heights[i] = serial.GetRootAsCommit([]byte(parents[i].(types.SerialMessage)), 0).Height()
		}
		bs, height := commit_flatbuffer(r.TargetHash(), opts, heights)
		v := types.SerialMessage(bs)
		addr := hash.Of(bs)
		return &Commit{v, addr, height}, nil
	}

	metaSt, err := opts.Meta.toNomsStruct(vrw.Format())
	if err != nil {
		return nil, err
	}

	refs := make([]types.Value, len(opts.Parents))
	for i, h := range opts.Parents {
		commitSt, err := vrw.ReadValue(ctx, h)
		if err != nil {
			return nil, err
		}
		if commitSt == nil {
			panic("parent not found " + h.String())
		}
		ref, err := types.NewRef(commitSt, vrw.Format())
		if err != nil {
			return nil, err
		}
		refs[i] = ref
	}
	parentsList, err := types.NewList(ctx, vrw, refs...)
	if err != nil {
		return nil, err
	}

	parentsClosure, includeParentsClosure, err := getParentsClosure(ctx, vrw, parentsList)
	if err != nil {
		return nil, err
	}

	cv, err := newCommit(ctx, v, parentsList, parentsClosure, includeParentsClosure, metaSt)
	if err != nil {
		return nil, err
	}
	r, err := types.NewRef(cv, vrw.Format())
	if err != nil {
		return nil, err
	}
	return &Commit{cv, r.TargetHash(), r.Height()}, nil
}

func commitPtr(nbf *types.NomsBinFormat, v types.Value, r *types.Ref) (*Commit, error) {
	if nbf == types.Format_DOLT_DEV {
		bs := []byte(v.(types.SerialMessage))
		height := serial.GetRootAsCommit(bs, 0).Height()
		var addr hash.Hash
		if r != nil {
			addr = r.TargetHash()
		} else {
			addr = hash.Of(bs)
		}
		return &Commit{
			val:    v,
			height: height,
			addr:   addr,
		}, nil
	}
	if r == nil {
		rv, err := types.NewRef(v, nbf)
		if err != nil {
			return nil, err
		}
		r = &rv
	}
	return &Commit{
		val:    v,
		height: r.Height(),
		addr:   r.TargetHash(),
	}, nil
}

func CommitFromValue(nbf *types.NomsBinFormat, v types.Value) (*Commit, error) {
	return commitPtr(nbf, v, nil)
}

func LoadCommitRef(ctx context.Context, vr types.ValueReader, r types.Ref) (*Commit, error) {
	v, err := vr.ReadValue(ctx, r.TargetHash())
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errors.New("target commit not found")
	}
	return commitPtr(vr.Format(), v, &r)
}

func LoadCommitAddr(ctx context.Context, vr types.ValueReader, addr hash.Hash) (*Commit, error) {
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errors.New("target commit not found")
	}
	return CommitFromValue(vr.Format(), v)
}

func findCommonAncestorUsingParentsList(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader) (hash.Hash, bool, error) {
	c1Q, c2Q := CommitByHeightHeap{c1}, CommitByHeightHeap{c2}
	for !c1Q.Empty() && !c2Q.Empty() {
		c1Ht, c2Ht := c1Q.MaxHeight(), c2Q.MaxHeight()
		if c1Ht == c2Ht {
			c1Parents, c2Parents := c1Q.PopCommitsOfHeight(c1Ht), c2Q.PopCommitsOfHeight(c2Ht)
			if common, ok := findCommonCommit(c1Parents, c2Parents); ok {
				return common.Addr(), true, nil
			}
			err := parentsToQueue(ctx, c1Parents, &c1Q, vr1)
			if err != nil {
				return hash.Hash{}, false, err
			}
			err = parentsToQueue(ctx, c2Parents, &c2Q, vr2)
			if err != nil {
				return hash.Hash{}, false, err
			}
		} else if c1Ht > c2Ht {
			err := parentsToQueue(ctx, c1Q.PopCommitsOfHeight(c1Ht), &c1Q, vr1)
			if err != nil {
				return hash.Hash{}, false, err
			}
		} else {
			err := parentsToQueue(ctx, c2Q.PopCommitsOfHeight(c2Ht), &c2Q, vr2)
			if err != nil {
				return hash.Hash{}, false, err
			}
		}
	}

	return hash.Hash{}, false, nil
}

// FindCommonAncestor returns the most recent common ancestor of c1 and c2, if
// one exists, setting ok to true. If there is no common ancestor, ok is set
// to false. Refs of |c1| are dereferenced through |vr1|, while refs of |c2|
// are dereference through |vr2|.
//
// This implementation makes use of the parents_closure field on the commit
// struct.  If the commit does not have a materialized parents_closure, this
// implementation delegates to findCommonAncestorUsingParentsList.
func FindCommonAncestor(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader) (hash.Hash, bool, error) {
	pi1, err := newParentsClosureIterator(ctx, c1, vr1)
	if err != nil {
		return hash.Hash{}, false, err
	}
	if pi1 == nil {
		return findCommonAncestorUsingParentsList(ctx, c1, c2, vr1, vr2)
	}

	pi2, err := newParentsClosureIterator(ctx, c2, vr2)
	if err != nil {
		return hash.Hash{}, false, err
	}
	if pi2 == nil {
		return findCommonAncestorUsingParentsList(ctx, c1, c2, vr1, vr2)
	}

	for {
		h1, h2 := pi1.Hash(), pi2.Hash()
		if h1 == h2 {
			if err := firstError(pi1.Err(), pi2.Err()); err != nil {
				return hash.Hash{}, false, err
			}
			return h1, true, nil
		}
		if pi1.Less(vr1.Format(), pi2) {
			// TODO: Should pi2.Seek(pi1.curr), but MapIterator does not expose Seek yet.
			if !pi2.Next(ctx) {
				return hash.Hash{}, false, firstError(pi1.Err(), pi2.Err())
			}
		} else {
			if !pi1.Next(ctx) {
				return hash.Hash{}, false, firstError(pi1.Err(), pi2.Err())
			}
		}
	}
}

// FindClosureCommonAncestor returns the most recent common ancestor of |cl| and |cm|,
// where |cl| is the transitive closure of one or more refs. If a common ancestor
// exists, |ok| is set to true, else false.
func FindClosureCommonAncestor(ctx context.Context, cl CommitClosure, cm *Commit, vr types.ValueReader) (a hash.Hash, ok bool, err error) {
	q := &CommitByHeightHeap{cm}
	var curr []*Commit

	for !q.Empty() {
		curr = q.PopCommitsOfHeight(q.MaxHeight())

		for _, r := range curr {
			ok, err = cl.Contains(ctx, r)
			if err != nil {
				return hash.Hash{}, false, err
			}
			if ok {
				return r.Addr(), ok, nil
			}
		}

		err = parentsToQueue(ctx, curr, q, vr)
		if err != nil {
			return hash.Hash{}, false, err
		}
	}

	return hash.Hash{}, false, nil
}

// GetCommitParents returns |Ref|s to the parents of the commit.
func GetCommitParents(ctx context.Context, vr types.ValueReader, cv types.Value) ([]*Commit, error) {
	if sm, ok := cv.(types.SerialMessage); ok {
		data := []byte(sm)
		if serial.GetFileID(data) != serial.CommitFileID {
			return nil, errors.New("GetCommitParents: provided value is not a commit.")
		}
		refs, err := types.SerialCommitParentRefs(vr.Format(), sm)
		if err != nil {
			return nil, err
		}
		hashes := make([]hash.Hash, len(refs))
		for i, r := range refs {
			hashes[i] = r.TargetHash()
		}
		vals, err := vr.ReadManyValues(ctx, hashes)
		if err != nil {
			return nil, err
		}
		res := make([]*Commit, len(vals))
		for i, v := range vals {
			if v == nil {
				return nil, fmt.Errorf("GetCommitParents: Did not find parent Commit in ValueReader: %s", hashes[i].String())
			}
			csm := serial.GetRootAsCommit([]byte(v.(types.SerialMessage)), 0)
			res[i] = &Commit{
				val:    v,
				height: csm.Height(),
				addr:   hashes[i],
			}
		}
		return res, nil
	}
	c, ok := cv.(types.Struct)
	if !ok {
		return nil, errors.New("GetCommitParents: provided value is not a commit.")
	}
	if c.Name() != commitName {
		return nil, errors.New("GetCommitParents: provided value is not a commit.")
	}
	var refs []types.Ref
	ps, ok, err := c.MaybeGet(parentsListField)
	if err != nil {
		return nil, err
	}
	if ok {
		p := ps.(types.List)
		err = p.IterAll(ctx, func(v types.Value, _ uint64) error {
			refs = append(refs, v.(types.Ref))
			return nil
		})
	} else {
		ps, ok, err = c.MaybeGet(parentsField)
		if err != nil {
			return nil, err
		}
		if ok {
			p := ps.(types.Set)
			err = p.IterAll(ctx, func(v types.Value) error {
				refs = append(refs, v.(types.Ref))
				return nil
			})
		}
	}
	hashes := make([]hash.Hash, len(refs))
	for i, r := range refs {
		hashes[i] = r.TargetHash()
	}
	vals, err := vr.ReadManyValues(ctx, hashes)
	if err != nil {
		return nil, err
	}
	res := make([]*Commit, len(refs))
	for i, val := range vals {
		if val == nil {
			return nil, fmt.Errorf("GetCommitParents: Did not find parent Commit in ValueReader: %s", hashes[i].String())
		}
		res[i] = &Commit{
			val:    val,
			height: refs[i].Height(),
			addr:   refs[i].TargetHash(),
		}
	}
	return res, nil
}

// GetCommitMeta extracts the CommitMeta field from a commit. Returns |nil,
// nil| if there is no metadata for the commit.
func GetCommitMeta(ctx context.Context, cv types.Value) (*CommitMeta, error) {
	if sm, ok := cv.(types.SerialMessage); ok {
		data := []byte(sm)
		if serial.GetFileID(data) != serial.CommitFileID {
			return nil, errors.New("GetCommitMeta: provided value is not a commit.")
		}
		cmsg := serial.GetRootAsCommit(data, 0)
		ret := &CommitMeta{}
		ret.Name = string(cmsg.Name())
		ret.Email = string(cmsg.Email())
		ret.Description = string(cmsg.Description())
		ret.Timestamp = cmsg.TimestampMillis()
		ret.UserTimestamp = cmsg.UserTimestampMillis()
		return ret, nil
	}
	c, ok := cv.(types.Struct)
	if !ok {
		return nil, errors.New("GetCommitMeta: provided value is not a commit.")
	}
	if c.Name() != commitName {
		return nil, errors.New("GetCommitMeta: provided value is not a commit.")
	}
	metaVal, found, err := c.MaybeGet(commitMetaField)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	if metaSt, ok := metaVal.(types.Struct); ok {
		return CommitMetaFromNomsSt(metaSt)
	} else {
		return nil, errors.New("GetCommitMeta: Commit had metadata field but it was not a Struct.")
	}
}

func GetCommittedValue(ctx context.Context, vr types.ValueReader, cv types.Value) (types.Value, error) {
	if sm, ok := cv.(types.SerialMessage); ok {
		data := []byte(sm)
		if serial.GetFileID(data) != serial.CommitFileID {
			return nil, errors.New("GetCommittedValue: provided value is not a commit.")
		}
		cmsg := serial.GetRootAsCommit(data, 0)
		var roothash hash.Hash
		copy(roothash[:], cmsg.RootBytes())
		return vr.ReadValue(ctx, roothash)
	}
	c, ok := cv.(types.Struct)
	if !ok {
		return nil, errors.New("GetCommittedValue: provided value is not a commit.")
	}
	if c.Name() != commitName {
		return nil, errors.New("GetCommittedValue: provided value is not a commit.")
	}
	v, _, err := c.MaybeGet(valueField)
	return v, err
}

func parentsToQueue(ctx context.Context, commits []*Commit, q *CommitByHeightHeap, vr types.ValueReader) error {
	seen := make(map[hash.Hash]bool)
	for _, c := range commits {
		if _, ok := seen[c.Addr()]; ok {
			continue
		}
		seen[c.Addr()] = true

		parents, err := GetCommitParents(ctx, vr, c.NomsValue())
		if err != nil {
			return err
		}
		for _, r := range parents {
			heap.Push(q, r)
		}
	}

	return nil
}

func findCommonCommit(a, b []*Commit) (*Commit, bool) {
	toAddrMap := func(s []*Commit) map[hash.Hash]*Commit {
		out := map[hash.Hash]*Commit{}
		for _, r := range s {
			out[r.Addr()] = r
		}
		return out
	}

	as, bs := toAddrMap(a), toAddrMap(b)
	for s, c := range as {
		if _, present := bs[s]; present {
			return c, true
		}
	}
	return nil, false
}

func makeCommitStructType(metaType, parentsType, parentsListType, parentsClosureType, valueType *types.Type, includeParentsClosure bool) (*types.Type, error) {
	if includeParentsClosure {
		return types.MakeStructType(commitName,
			types.StructField{
				Name: commitMetaField,
				Type: metaType,
			},
			types.StructField{
				Name: parentsField,
				Type: parentsType,
			},
			types.StructField{
				Name: parentsListField,
				Type: parentsListType,
			},
			types.StructField{
				Name: parentsClosureField,
				Type: parentsClosureType,
			},
			types.StructField{
				Name: valueField,
				Type: valueType,
			},
		)
	} else {
		return types.MakeStructType(commitName,
			types.StructField{
				Name: commitMetaField,
				Type: metaType,
			},
			types.StructField{
				Name: parentsField,
				Type: parentsType,
			},
			types.StructField{
				Name: parentsListField,
				Type: parentsListType,
			},
			types.StructField{
				Name: valueField,
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

func commitToMapKeyTuple(f *types.NomsBinFormat, c *Commit) (types.Tuple, error) {
	h := c.Addr()
	ib := make([]byte, len(hash.Hash{}))
	copy(ib, h[:])
	return types.NewTuple(f, types.Uint(c.Height()), types.InlineBlob(ib))
}

func firstError(l, r error) error {
	if l != nil {
		return l
	}
	return r
}

func newParentsClosureIterator(ctx context.Context, c *Commit, vr types.ValueReader) (*parentsClosureIterator, error) {
	sv := c.NomsValue()

	if _, ok := sv.(types.SerialMessage); ok {
		// TODO: __DOLT_DEV__ should support parent closures.
		return nil, nil
	}

	s, ok := sv.(types.Struct)
	if !ok {
		return nil, fmt.Errorf("target ref is not struct: %v", sv)
	}
	if s.Name() != commitName {
		return nil, fmt.Errorf("target ref is not commit: %v", sv)
	}

	fv, ok, err := s.MaybeGet(parentsClosureField)
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

	initialCurr, err := commitToMapKeyTuple(vr.Format(), c)
	if err != nil {
		return nil, err
	}

	return &parentsClosureIterator{mi, nil, initialCurr}, nil
}

func IsCommitType(nbf *types.NomsBinFormat, t *types.Type) bool {
	return types.IsSubtype(nbf, valueCommitType, t)
}

func IsCommit(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); ok {
		return types.IsValueSubtypeOf(s.Format(), v, valueCommitType)
	} else if sm, ok := v.(types.SerialMessage); ok {
		data := []byte(sm)
		return serial.GetFileID(data) == serial.CommitFileID, nil
	} else {
		return false, nil
	}
}

type CommitByHeightHeap []*Commit

func (r CommitByHeightHeap) Less(i, j int) bool {
	if r[i].Height() == r[j].Height() {
		return r[i].Addr().Less(r[j].Addr())
	}
	return r[i].Height() > r[j].Height()
}

func (r CommitByHeightHeap) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r CommitByHeightHeap) Len() int {
	return len(r)
}

func (r *CommitByHeightHeap) Push(x interface{}) {
	*r = append(*r, x.(*Commit))
}

func (r *CommitByHeightHeap) Pop() interface{} {
	old := *r
	ret := old[len(old)-1]
	*r = old[:len(old)-1]
	return ret
}

func (r CommitByHeightHeap) Empty() bool {
	return len(r) == 0
}

func (r CommitByHeightHeap) MaxHeight() uint64 {
	return r[0].Height()
}

func (r *CommitByHeightHeap) PopCommitsOfHeight(h uint64) []*Commit {
	var ret []*Commit
	for !r.Empty() && r.MaxHeight() == h {
		ret = append(ret, heap.Pop(r).(*Commit))
	}
	return ret
}
