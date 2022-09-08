// Copyright 2021 Dolthub, Inc.
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
	"io"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	maxPending = 64 * 1024
)

// MutableMap represents a Map that is able to store mutations in-memory. A MutableMap has two tiers of in-memory storage:
// pending and applied. All mutations are first written to the pending tier, which may be discarded at any time.
// However, once ApplyPending() is called, those mutations are moved to the applied tier, and the pending tier is
// cleared.
type MutableMap struct {
	tuples  tree.MutableMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

// newMutableMap returns a new MutableMap.
func newMutableMap(m Map) MutableMap {
	return MutableMap{
		tuples:  m.tuples.Mutate(),
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}
}

// Map materializes all pending and applied mutations in the MutableMap.
func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	s := message.NewProllyMapSerializer(mut.valDesc, mut.NodeStore().Pool())
	return mut.flushWithSerializer(ctx, s)
}

func (mut MutableMap) flushWithSerializer(ctx context.Context, s message.Serializer) (Map, error) {
	if err := mut.ApplyPending(ctx); err != nil {
		return Map{}, err
	}

	sm := mut.tuples.StaticMap
	fn := tree.ApplyMutations[val.Tuple, val.TupleDesc, message.Serializer]

	root, err := fn(ctx, sm.NodeStore, sm.Root, mut.keyDesc, s, mut.tuples.Mutations())
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples: tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
			Root:      root,
			NodeStore: sm.NodeStore,
			Order:     sm.Order,
		},
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}, nil
}

// NodeStore returns the map's NodeStore
func (mut MutableMap) NodeStore() tree.NodeStore {
	return mut.tuples.StaticMap.NodeStore
}

// Put adds the Tuple pair |key|, |value| to the MutableMap.
func (mut MutableMap) Put(ctx context.Context, key, value val.Tuple) error {
	return mut.tuples.Put(ctx, key, value)
}

// Delete deletes the pair keyed by |key| from the MutableMap.
func (mut MutableMap) Delete(ctx context.Context, key val.Tuple) error {
	return mut.tuples.Delete(ctx, key)
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MutableMap, a nil Tuple pair is passed to |cb|.
func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return mut.tuples.Get(ctx, key, cb)
}

// Has returns true if |key| is present in the MutableMap.
func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return mut.tuples.Has(ctx, key)
}

// ApplyPending moves all pending mutations to the underlying map.
func (mut *MutableMap) ApplyPending(ctx context.Context) error {
	mut.tuples.Edits.Checkpoint()
	return nil
}

// DiscardPending removes all pending mutations.
func (mut *MutableMap) DiscardPending(context.Context) {
	mut.tuples.Edits.Revert()
}

// IterAll returns a mutableMapIter that iterates over the entire MutableMap.
func (mut MutableMap) IterAll(ctx context.Context) (MapIter, error) {
	rng := Range{Fields: nil, Desc: mut.keyDesc}
	return mut.IterRange(ctx, rng)
}

// IterRange returns a MapIter that iterates over a Range.
func (mut MutableMap) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	treeIter, err := treeIterFromRange(ctx, mut.tuples.StaticMap.Root, mut.tuples.StaticMap.NodeStore, rng)
	if err != nil {
		return nil, err
	}
	memIter := memIterFromRange(mut.tuples.Edits, rng)

	iter := &mutableMapIter[val.Tuple, val.Tuple, val.TupleDesc]{
		memory: memIter,
		prolly: treeIter,
		order:  rng.Desc,
	}

	return filteredIter{iter: iter, rng: rng}, err
}

// HasEdits returns true when the MutableMap has performed at least one Put or Delete operation. This does not indicate
// whether the materialized map contains different values to the contained unedited map.
func (mut MutableMap) HasEdits() bool {
	return mut.tuples.Edits.Count() > 0
}

// Descriptors returns the key and value val.TupleDesc.
func (mut MutableMap) Descriptors() (val.TupleDesc, val.TupleDesc) {
	return mut.keyDesc, mut.valDesc
}

func debugFormat(ctx context.Context, m MutableMap) (string, error) {
	kd, vd := m.keyDesc, m.valDesc

	editIter := m.tuples.Edits.IterAtStart()
	tupleIter, err := m.tuples.StaticMap.IterAll(ctx)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("Mutable Map {\n")

	c := strconv.Itoa(m.tuples.Edits.Count())
	sb.WriteString("\tedits (count: " + c + ") {\n")
	for {
		k, v := editIter.Current()
		if k == nil {
			break
		}
		sb.WriteString("\t\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(v))
		sb.WriteString(",\n")
		editIter.Advance()
	}
	sb.WriteString("\t},\n")

	ci, err := m.tuples.StaticMap.Count()
	if err != nil {
		return "", err
	}

	c = strconv.Itoa(ci)
	sb.WriteString("\tTree (count: " + c + ") {\n")
	for {
		k, v, err := tupleIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		sb.WriteString("\t\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(v))
		sb.WriteString(",\n")
	}
	sb.WriteString("\t}\n}\n")
	return sb.String(), nil
}

type tupleIter struct {
	tuples []val.Tuple
}

var _ TupleIter = &tupleIter{}

func (s *tupleIter) Next(context.Context) (k, v val.Tuple) {
	if len(s.tuples) > 0 {
		k, v = s.tuples[0], s.tuples[1]
		s.tuples = s.tuples[2:]
	}
	return
}

// mutationIter wraps a TupleIter as a MutationIter.
type mutationIter struct {
	iter TupleIter
}

var _ tree.MutationIter = mutationIter{}

func (m mutationIter) NextMutation(ctx context.Context) (key, value tree.Item) {
	k, v := m.iter.Next(ctx)
	key, value = tree.Item(k), tree.Item(v)
	return
}

func (m mutationIter) Close() error {
	return nil
}
