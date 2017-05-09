// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// candidate represents a collection that is a candidate to be merged. This
// interface exists to wrap Maps, Sets and Structs with a common API so that
// threeWayOrderedSequenceMerge() can remain agnostic to which kind of
// collections it's actually working with.
type candidate interface {
	diff(parent candidate, change chan<- types.ValueChanged, stop <-chan struct{})
	get(k types.Value) types.Value
	pathConcat(change types.ValueChanged, path types.Path) (out types.Path)
	getValue() types.Value
}

type mapCandidate struct {
	m types.Map
}

func (mc mapCandidate) diff(p candidate, change chan<- types.ValueChanged, stop <-chan struct{}) {
	mc.m.Diff(p.(mapCandidate).m, change, stop)
}

func (mc mapCandidate) get(k types.Value) types.Value {
	return mc.m.Get(k)
}

func (mc mapCandidate) pathConcat(change types.ValueChanged, path types.Path) (out types.Path) {
	out = append(out, path...)
	if kind := change.Key.Kind(); kind == types.BoolKind || kind == types.StringKind || kind == types.NumberKind {
		out = append(out, types.NewIndexPath(change.Key))
	} else {
		out = append(out, types.NewHashIndexPath(change.Key.Hash()))
	}
	return
}

func (mc mapCandidate) getValue() types.Value {
	return mc.m
}

type setCandidate struct {
	s types.Set
}

func (sc setCandidate) diff(p candidate, change chan<- types.ValueChanged, stop <-chan struct{}) {
	sc.s.Diff(p.(setCandidate).s, change, stop)
}

func (sc setCandidate) get(k types.Value) types.Value {
	return k
}

func (sc setCandidate) pathConcat(change types.ValueChanged, path types.Path) (out types.Path) {
	out = append(out, path...)
	if kind := change.Key.Kind(); kind == types.BoolKind || kind == types.StringKind || kind == types.NumberKind {
		out = append(out, types.NewIndexPath(change.Key))
	} else {
		out = append(out, types.NewHashIndexPath(change.Key.Hash()))
	}
	return
}

func (sc setCandidate) getValue() types.Value {
	return sc.s
}

type structCandidate struct {
	s types.Struct
}

func (sc structCandidate) diff(p candidate, change chan<- types.ValueChanged, stop <-chan struct{}) {
	sc.s.Diff(p.(structCandidate).s, change, stop)
}

func (sc structCandidate) get(key types.Value) types.Value {
	if field, ok := key.(types.String); ok {
		val, _ := sc.s.MaybeGet(string(field))
		return val
	}
	panic(fmt.Errorf("Bad key type in diff: %s", types.TypeOf(key).Describe()))
}

func (sc structCandidate) pathConcat(change types.ValueChanged, path types.Path) (out types.Path) {
	out = append(out, path...)
	str, ok := change.Key.(types.String)
	if !ok {
		d.Panic("Field names must be strings, not %s", types.TypeOf(change.Key).Describe())
	}
	return append(out, types.NewFieldPath(string(str)))
}

func (sc structCandidate) getValue() types.Value {
	return sc.s
}
