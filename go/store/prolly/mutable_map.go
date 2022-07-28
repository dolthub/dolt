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
	tuples  orderedMap[val.Tuple, val.Tuple, val.TupleDesc]
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

// newMutableMap returns a new MutableMap.
func newMutableMap(m Map) MutableMap {
	return MutableMap{
		tuples:  m.tuples.mutate(),
		keyDesc: m.keyDesc,
		valDesc: m.valDesc,
	}
}

// Map materializes all pending and applied mutations in the MutableMap.
func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	serializer := message.ProllyMapSerializer{
		Pool:    mut.NodeStore().Pool(),
		ValDesc: mut.valDesc,
	}
	return mut.flushWithSerializer(ctx, serializer)
}

func (mut MutableMap) flushWithSerializer(ctx context.Context, s message.Serializer) (Map, error) {
	if err := mut.ApplyPending(ctx); err != nil {
		return Map{}, err
	}

	tr := mut.tuples.tree
	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, s, mut.tuples.mutations(), tr.compareItems)
	if err != nil {
		return Map{}, err
	}

	return Map{
		tuples: orderedTree[val.Tuple, val.Tuple, val.TupleDesc]{
			root:  root,
			ns:    tr.ns,
			order: tr.order,
		},
		keyDesc: mut.keyDesc,
		valDesc: mut.valDesc,
	}, nil
}

// NodeStore returns the map's NodeStore
func (mut MutableMap) NodeStore() tree.NodeStore {
	return mut.tuples.tree.ns
}

// Put adds the Tuple pair |key|, |value| to the MutableMap.
func (mut MutableMap) Put(ctx context.Context, key, value val.Tuple) error {
	return mut.tuples.put(ctx, key, value)
}

// Delete deletes the pair keyed by |key| from the MutableMap.
func (mut MutableMap) Delete(ctx context.Context, key val.Tuple) error {
	return mut.tuples.delete(ctx, key)
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MutableMap, a nil Tuple pair is passed to |cb|.
func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return mut.tuples.get(ctx, key, cb)
}

// Has returns true if |key| is present in the MutableMap.
func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return mut.tuples.has(ctx, key)
}

// ApplyPending moves all pending mutations to the underlying map.
func (mut *MutableMap) ApplyPending(ctx context.Context) error {
	mut.tuples.edits.Checkpoint()
	return nil
}

// DiscardPending removes all pending mutations.
func (mut *MutableMap) DiscardPending(context.Context) {
	mut.tuples.edits.Revert()
}

// IterAll returns a mutableMapIter that iterates over the entire MutableMap.
func (mut MutableMap) IterAll(ctx context.Context) (MapIter, error) {
	rng := Range{Fields: nil, Desc: mut.keyDesc}
	return mut.IterRange(ctx, rng)
}

// IterRange returns a MapIter that iterates over a Range.
func (mut MutableMap) IterRange(ctx context.Context, rng Range) (MapIter, error) {
	treeIter, err := treeIterFromRange(ctx, mut.tuples.tree.root, mut.tuples.tree.ns, rng)
	if err != nil {
		return nil, err
	}
	memIter := memIterFromRange(mut.tuples.edits, rng)

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
	return mut.tuples.edits.Count() > 0
}

func debugFormat(ctx context.Context, m MutableMap) (string, error) {
	kd, vd := m.keyDesc, m.valDesc

	editIter := m.tuples.edits.IterAtStart()
	tupleIter, err := m.tuples.tree.iterAll(ctx)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("Mutable Map {\n")

	c := strconv.Itoa(m.tuples.edits.Count())
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

	c = strconv.Itoa(m.tuples.tree.count())
	sb.WriteString("\ttree (count: " + c + ") {\n")
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
