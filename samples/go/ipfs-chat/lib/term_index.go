// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"sync"

	"github.com/attic-labs/noms/go/types"
)

type TermIndex struct {
	TermDocs types.Map
}

func NewTermIndex(TermDocs types.Map) TermIndex {
	return TermIndex{TermDocs}
}

func (ti TermIndex) Edit() *TermIndexEditor {
	return &TermIndexEditor{ti.TermDocs.Edit()}
}

func (ti TermIndex) Search(terms []string) types.Map {
	seen := make(map[string]struct{}, len(terms))
	iters := make([]types.SetIterator, 0, len(terms))

	wg := sync.WaitGroup{}
	idx := 0
	for _, t := range terms {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}

		iters = append(iters, nil)
		i := idx
		t := t
		wg.Add(1)
		go func() {
			ts := ti.TermDocs.Get(types.String(t))
			if ts != nil {
				iter := ts.(types.Set).Iterator()
				iters[i] = iter
			}
			wg.Done()
		}()

		idx++
	}
	wg.Wait()

	var si types.SetIterator
	for _, iter := range iters {
		if iter == nil {
			return types.NewMap() // at least one term had no hits
		}

		if si == nil {
			si = iter // first iter
			continue
		}

		si = types.NewIntersectionIterator(si, iter)
	}

	ch := make(chan types.Value)
	rch := types.NewStreamingMap(nil, ch)
	for next := si.Next(); next != nil; next = si.Next() {
		ch <- next
		ch <- types.Bool(true)
	}
	close(ch)

	return <-rch
}

type TermIndexEditor struct {
	terms *types.MapEditor
}

// Builds a new TermIndex
func (te *TermIndexEditor) Value(vrw types.ValueReadWriter) TermIndex {
	return TermIndex{te.terms.Map(vrw)}
}

// Indexes |v| by |term|
func (te *TermIndexEditor) Insert(term string, v types.Value) *TermIndexEditor {
	tv := types.String(term)
	hitSet := te.terms.Get(tv)
	if hitSet == nil {
		hitSet = types.NewSet()
	}
	hsEd, ok := hitSet.(*types.SetEditor)
	if !ok {
		hsEd = hitSet.(types.Set).Edit()
		te.terms.Set(tv, hsEd)
	}

	hsEd.Insert(v)
	return te
}

// Indexes |v| by each unique term in |terms| (tolerates duplicate terms)
func (te *TermIndexEditor) InsertAll(terms []string, v types.Value) *TermIndexEditor {
	visited := map[string]struct{}{}
	for _, term := range terms {
		if _, ok := visited[term]; ok {
			continue
		}
		visited[term] = struct{}{}
		te.Insert(term, v)
	}
	return te
}

// TODO: te.Remove
