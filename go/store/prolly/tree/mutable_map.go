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

package tree

import (
	"context"

	"github.com/dolthub/dolt/go/store/skip"
)

// MutableMap is a mutable prolly Static with ordered elements.
type MutableMap[K, V ~[]byte, O Ordering[K], M MapInterface[K, V, O]] struct {
	Edits  *skip.List
	Static M
}

func (m MutableMap[K, V, O, M]) Put(ctx context.Context, key K, value V) error {
	return m.Edits.Put(ctx, key, value)
}

func (m MutableMap[K, V, O, M]) Delete(ctx context.Context, key K) error {
	return m.Edits.Put(ctx, key, nil)
}

func (m MutableMap[K, V, O, M]) Get(ctx context.Context, key K, cb KeyValueFn[K, V]) (err error) {
	value, ok, err := m.Edits.Get(ctx, key)
	if err != nil {
		return err
	}
	if ok {
		if value == nil {
			key = nil // there is a pending delete of |key| in |m.Edits|.
		}
		return cb(key, value)
	}

	return m.Static.Get(ctx, key, cb)
}

func (m MutableMap[K, V, O, M]) GetPrefix(ctx context.Context, key K, prefixOrder O, cb KeyValueFn[K, V]) (err error) {
	iter, err := m.Edits.GetIterFromSeekFn(func(k []byte) (advance bool, err error) {
		if k != nil { // seek until |k| >= |key|
			cmp, err := prefixOrder.Compare(ctx, k, key)
			if err != nil {
				return false, err
			}
			advance = cmp < 0
		}
		return
	})
	if err != nil {
		return err
	}
	k, v := iter.Current()
	if k != nil {
		cmp, err := prefixOrder.Compare(ctx, k, key)
		if err != nil {
			return err
		}
		if cmp == 0 {
			if v == nil {
				k = nil // there is a pending delete of |key| in |m.Edits|.
			}
			return cb(k, v)
		}
	}
	return m.Static.GetPrefix(ctx, key, prefixOrder, cb)
}

func (m MutableMap[K, V, O, M]) Has(ctx context.Context, key K) (present bool, err error) {
	value, ok, err := m.Edits.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if ok {
		present = value != nil
		return
	}
	return m.Static.Has(ctx, key)
}

func (m MutableMap[K, V, O, M]) HasPrefix(ctx context.Context, key K, prefixOrder O) (present bool, err error) {
	iter, err := m.Edits.GetIterFromSeekFn(func(k []byte) (advance bool, err error) {
		if k != nil { // seek until |k| >= |key|
			cmp, err := prefixOrder.Compare(ctx, k, key)
			if err != nil {
				return false, err
			}
			advance = cmp < 0
		}
		return
	})
	if err != nil {
		return false, err
	}
	k, v := iter.Current()
	if k != nil {
		cmp, cmpErr := prefixOrder.Compare(ctx, k, key)
		if cmpErr != nil {
			return false, cmpErr
		}
		if cmp == 0 {
			present = v != nil
			return
		}
	}
	return m.Static.HasPrefix(ctx, key, prefixOrder)
}

func (m MutableMap[K, V, O, M]) Copy() MutableMap[K, V, O, M] {
	return MutableMap[K, V, O, M]{
		Edits:  m.Edits.Copy(),
		Static: m.Static,
	}
}

func (m MutableMap[K, V, O, M]) Mutations() MutationIter {
	return orderedListIter[K, V]{iter: m.Edits.IterAtStart()}
}

type orderedListIter[K, V ~[]byte] struct {
	iter *skip.ListIter
}

var _ MutationIter = &orderedListIter[Item, Item]{}

func (it orderedListIter[K, V]) NextMutation(context.Context) (mutation Mutation) {
	mutation.Key, mutation.Value = it.iter.Current()
	if mutation.Key == nil {
		return mutation
	}
	it.iter.Advance()
	return mutation
}

func (it orderedListIter[K, V]) Close() error {
	return nil
}
