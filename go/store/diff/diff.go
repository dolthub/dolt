// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"context"
	"sync/atomic"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type (
	diffFunc     func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{})
	pathPartFunc func(v types.Value) types.PathPart
	valueFunc    func(k types.Value) types.Value
)

// Difference represents a "diff" between two Noms graphs.
type Difference struct {
	// Path to the Value that has changed
	Path types.Path
	// ChangeType indicates the type of diff: modified, added, deleted
	ChangeType types.DiffChangeType
	// OldValue is Value before the change, can be nil if Value was added
	OldValue types.Value
	// NewValue is Value after the change, can be nil if Value was removed
	NewValue types.Value
	// NewKeyValue is used for when elements are added to diffs with a
	// non-primitive key. The new key must available when the map gets updated.
	NewKeyValue types.Value
	// KeyValue holds the key associated with a changed map value
	KeyValue types.Value
}

func (dif Difference) IsEmpty() bool {
	return dif.Path == nil && dif.OldValue == nil && dif.NewValue == nil
}

type ShouldDescFunc func(v1, v2 types.Value) bool

// differ is used internally to hold information necessary for diffing two graphs.
type differ struct {
	// Channel used to send Difference objects back to caller
	diffChan chan<- Difference
	// Channel that caller should close() to terminate Diff function.
	stopChan chan struct{}
	// Use LeftRight diff as opposed to TopDown
	leftRight bool

	shouldDescend ShouldDescFunc
}

// Diff traverses two graphs simultaneously looking for differences. It returns
// two channels: a DiffReceiveChan that the caller can use to iterate over the
// diffs in the graph and a StopSendChanel that a caller can use to signal the
// Diff function to stop processing.
// Diff returns the Differences in depth-first first order. A 'diff' is defined
// as one of the following conditions:
//  * a Value is Added or Removed from a node in the graph
//  * the type of a Value has changed in the graph
//  * a primitive (i.e. Bool, Float, String, Ref or Blob) Value has changed.
//
// A Difference is not returned when a non-primitive value has been modified. For
// example, a struct field has been changed from one Value of type Employee to
// another. Those modifications are accounted for by the Differences described
// above at a lower point in the graph.
//
// If leftRight is true then the left-right diff is used for ordered sequences
// - see Diff vs DiffLeftRight in Set and Map.
//
// Note: the function sends messages on diffChan and checks whether stopChan has
// been closed to know if it needs to terminate diffing early. To function
// properly it needs to be executed concurrently with code that reads values from
// diffChan. The following is a typical invocation of Diff():
//    dChan := make(chan Difference)
//    sChan := make(chan struct{})
//    go func() {
//        d.Diff(s3, s4, dChan, sChan, leftRight)
//        close(dChan)
//    }()
//    for dif := range dChan {
//        <some code>
//    }
func Diff(ctx context.Context, v1, v2 types.Value, dChan chan<- Difference, stopChan chan struct{}, leftRight bool, descFunc ShouldDescFunc) {
	if descFunc == nil {
		descFunc = ShouldDescend
	}

	d := differ{diffChan: dChan, stopChan: stopChan, leftRight: leftRight, shouldDescend: descFunc}
	if !v1.Equals(v2) {
		if !d.shouldDescend(v1, v2) {
			d.sendDiff(Difference{Path: nil, ChangeType: types.DiffChangeModified, OldValue: v1, NewValue: v2})
		} else {
			d.diff(ctx, nil, v1, v2)
		}
	}
}

func (d differ) diff(ctx context.Context, p types.Path, v1, v2 types.Value) bool {
	switch v1.Kind() {
	case types.ListKind:
		return d.diffLists(ctx, p, v1.(types.List), v2.(types.List))
	case types.MapKind:
		return d.diffMaps(ctx, p, v1.(types.Map), v2.(types.Map))
	case types.SetKind:
		return d.diffSets(ctx, p, v1.(types.Set), v2.(types.Set))
	case types.StructKind:
		return d.diffStructs(ctx, p, v1.(types.Struct), v2.(types.Struct))
	default:
		panic("Unrecognized type in diff function")
	}
}

func (d differ) diffLists(ctx context.Context, p types.Path, v1, v2 types.List) (stop bool) {
	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1s, so this won't block if diff already finished

	var rp atomic.Value
	go func() {
		defer close(spliceChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		v2.Diff(ctx, v1, spliceChan, stopChan)
	}()

	for splice := range spliceChan {
		if stop {
			break
		}
		if splice.SpRemoved == splice.SpAdded {
			// Heuristic: list only has modifications.
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl := v1.Get(ctx, splice.SpAt+i)
				newEl := v2.Get(ctx, splice.SpFrom+i)
				if d.shouldDescend(lastEl, newEl) {
					idx := types.Float(splice.SpAt + i)
					stop = d.diff(ctx, append(p, types.NewIndexPath(idx)), lastEl, newEl)
				} else {
					p1 := p.Append(types.NewIndexPath(types.Float(splice.SpAt + i)))
					dif := Difference{Path: p1, ChangeType: types.DiffChangeModified, OldValue: v1.Get(ctx, splice.SpAt+i), NewValue: v2.Get(ctx, splice.SpFrom+i)}
					stop = !d.sendDiff(dif)
				}
			}
			continue
		}

		// Heuristic: list only has additions/removals.
		for i := uint64(0); i < splice.SpRemoved && !stop; i++ {
			p1 := p.Append(types.NewIndexPath(types.Float(splice.SpAt + i)))
			dif := Difference{Path: p1, ChangeType: types.DiffChangeRemoved, OldValue: v1.Get(ctx, splice.SpAt+i), NewValue: nil}
			stop = !d.sendDiff(dif)
		}
		for i := uint64(0); i < splice.SpAdded && !stop; i++ {
			p1 := p.Append(types.NewIndexPath(types.Float(splice.SpFrom + i)))
			dif := Difference{Path: p1, ChangeType: types.DiffChangeAdded, OldValue: nil, NewValue: v2.Get(ctx, splice.SpFrom+i)}
			stop = !d.sendDiff(dif)
		}
	}

	if stop {
		stopChan <- struct{}{}
		// Wait for diff to stop.
		for range spliceChan {
		}
	}

	if r := rp.Load(); r != nil {
		panic(r)
	}

	return
}

func (d differ) diffMaps(ctx context.Context, p types.Path, v1, v2 types.Map) bool {
	return d.diffOrdered(ctx, p,
		func(v types.Value) types.PathPart {
			if types.ValueCanBePathIndex(v) {
				return types.NewIndexPath(v)
			} else {
				return types.NewHashIndexPath(v.Hash())
			}
		},
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			if d.leftRight {
				v2.DiffLeftRight(ctx, v1, cc, sc)
			} else {
				v2.DiffHybrid(ctx, v1, cc, sc)
			}
		},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(ctx, k) },
		func(k types.Value) types.Value { return v2.Get(ctx, k) },
	)
}

func (d differ) diffStructs(ctx context.Context, p types.Path, v1, v2 types.Struct) bool {
	str := func(v types.Value) string {
		return string(v.(types.String))
	}
	return d.diffOrdered(ctx, p,
		func(v types.Value) types.PathPart { return types.NewFieldPath(str(v)) },
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			v2.Diff(v1, cc, sc)
		},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(str(k)) },
		func(k types.Value) types.Value { return v2.Get(str(k)) },
	)
}

func (d differ) diffSets(ctx context.Context, p types.Path, v1, v2 types.Set) bool {
	return d.diffOrdered(ctx, p,
		func(v types.Value) types.PathPart {
			if types.ValueCanBePathIndex(v) {
				return types.NewIndexPath(v)
			}
			return types.NewHashIndexPath(v.Hash())
		},
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			if d.leftRight {
				v2.DiffLeftRight(ctx, v1, cc, sc)
			} else {
				v2.DiffHybrid(ctx, v1, cc, sc)
			}
		},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return k },
	)
}

func (d differ) diffOrdered(ctx context.Context, p types.Path, ppf pathPartFunc, df diffFunc, kf, v1, v2 valueFunc) (stop bool) {
	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	var rp atomic.Value
	go func() {
		defer close(changeChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		df(changeChan, stopChan)
	}()

	for change := range changeChan {
		if stop {
			break
		}

		k := kf(change.Key)
		p1 := p.Append(ppf(k))

		switch change.ChangeType {
		case types.DiffChangeAdded:
			dif := Difference{Path: p1, ChangeType: types.DiffChangeAdded, OldValue: nil, NewValue: v2(change.Key), NewKeyValue: k, KeyValue: change.Key}
			stop = !d.sendDiff(dif)
		case types.DiffChangeRemoved:
			dif := Difference{Path: p1, ChangeType: types.DiffChangeRemoved, OldValue: v1(change.Key), NewValue: nil, KeyValue: change.Key}
			stop = !d.sendDiff(dif)
		case types.DiffChangeModified:
			c1, c2 := v1(change.Key), v2(change.Key)
			if d.shouldDescend(c1, c2) {
				stop = d.diff(ctx, p1, c1, c2)
			} else {
				dif := Difference{Path: p1, ChangeType: types.DiffChangeModified, OldValue: c1, NewValue: c2, KeyValue: change.Key}
				stop = !d.sendDiff(dif)
			}
		default:
			panic("unknown change type")
		}
	}

	if stop {
		stopChan <- struct{}{}
		for range changeChan {
		}
	}

	if r := rp.Load(); r != nil {
		panic(r)
	}

	return
}

// shouldDescend returns true, if Value is not primitive or is a Ref.
func ShouldDescend(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind == v2.Kind() && kind != types.RefKind && kind != types.TupleKind
}

// stopSent returns true if a message has been sent to this StopChannel
func (d differ) sendDiff(dif Difference) bool {
	select {
	case <-d.stopChan:
		return false
	case d.diffChan <- dif:
		return true
	}
}
