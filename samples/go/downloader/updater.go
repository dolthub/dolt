// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"sync"

	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/types"
)

// Update() and IncrementalUpdate() are useful for transforming a graph and then
// for efficiently re-applying the same transform function whenever the original
// graph is updated.
//
// For example, imagine a large dataset (let's call it Graph-A1) is imported
// into a Noms database. Then some update function 'U' is applied to that
// graph which results in graph: 'Graph-B1'. Over time data is added and changed
// in the original dataset and that gets imported as the next version of the
// original graph: Graph-A2. In most cases, we'll want to reapply the transform
// function to Graph-A2 and create Graph-B2 which now has the latest data with
// the update function applied to it. Here's how that make look in a diagram:
//
//   dataset1                                           dataset2
//
//   Graph-A2   ---->          U(Graph-A2)       ---->   Graph-B2
//     |                                                    |
//     |                                                    |
//     V                                                    V
//   Graph-A1   ---->          U(Graph-A1)       ---->   Graph-B1
//
// The problem here is that, if Graph-A1 is large and the diffs between Graph-A2
// and Graph-A1 are small, then Update(Graph-A2) is duplicating a lot of
// effort.
//
// IncrementalUpdate relies on diff.Diff() and diff.Apply() (see
// apply_patch.go) to do this more efficiently. Rather than applying the tranform
// function to the entire Graph-A2, IncrementalUpdate gets the Diff of Graph-A1
// and Graph-A2. It then applies the update function to just those diffs which
// creates a new "Patch" which can then be applied directly to Graph-B2 to
// generate Graph-B2. That results in a diagram like the one below:
//
//   dataset1                                           dataset2
//
//   Graph-A2   ----> U(Diff(GraphA2, Graph-A3)) ---->   Graph-B3
//     |                                                    |
//     |                                                    |
//     V                                                    V
//   Graph-A2   ----> U(Diff(GraphA1, Graph-A2)) ---->   Graph-B2
//     |                                                    |
//     |                                                    |
//     V                                                    V
//   Graph-A1   ---->         U(Graph-A1)        ---->   Graph-B1
//

// ShouldUpdateCallback defines a function that is called on each node in a
// graph. If it returns true, the node is added to a Difference object and sent
// to the 'found' channel for processing.
type ShouldUpdateCallback func(p types.Path, root, parent, value types.Value) bool

// UpdateCallback defines a function that takes a Difference and returns a
// modified difference that's suitable for patching into the target graph
type UpdateCallback func(diff diff.Difference) diff.Difference

// This function takes lastInRoot(GraphA1), inRoot(GraphA2) and
// lastOutRoot(GraphB1) as arguments and returns a types.Value(GraphB2). Invoking
// this function with either lastInRoot or lastOutRoot having a nil value is the
// same as calling Update directly.
func IncrementalUpdate(vr types.ValueReader, inRoot, lastInRoot, lastOutRoot types.Value, shouldUpdateCb ShouldUpdateCallback, updateCb UpdateCallback, concurrency uint) types.Value {
	if lastInRoot == nil || lastOutRoot == nil {
		return Update(vr, inRoot, shouldUpdateCb, updateCb, concurrency)
	}

	// Get the differences between lastInRoot and inRoot.
	dChan := make(chan diff.Difference, 128)
	sChan := make(chan struct{})
	go func() {
		diff.Diff(lastInRoot, inRoot, dChan, sChan, true)
		close(dChan)
	}()

	patch := diff.Patch{}
	for d := range dChan {
		// Transform each NewValue in Differences and add new diff to patch
		newValue := Update(vr, d.NewValue, shouldUpdateCb, updateCb, concurrency)

		// If newValue is nil, then call transform again on the oldValue because
		// we may need that to find an object to delete in the new graph.
		var oldValue types.Value
		if d.NewValue == nil {
			oldValue = Update(vr, d.OldValue, shouldUpdateCb, updateCb, concurrency)
		}
		dif := diff.Difference{Path: d.Path, ChangeType: d.ChangeType, OldValue: oldValue, NewValue: newValue}
		patch = append(patch, dif)
	}

	return diff.Apply(lastOutRoot, patch)
}

// Update walks through each node in a graph starting at root. It calls
// shouldUpdate() on each node it encounters. If true is returned it wraps the
// value in a Difference object and sends it to 'foundChan' to be 'transformed'
// by the caller. Any nodes that are sent to the foundChannel are processed by
// the caller and sent to 'updatedChan' when done.
// The 'concurrency' argument determines how many concurrent routines are
// are started for the updateCb to run in. If 'concurrency' > 1, updateCallbackCb
// must be thread-safe.
func Update(vr types.ValueReader, root types.Value, shouldUpdateCb ShouldUpdateCallback, updateCb UpdateCallback, concurrency uint) types.Value {
	foundChan := make(chan diff.Difference, 128)
	updatedChan := make(chan diff.Difference, 128)

	// reads all the updated Differences from updatedChan into this patch.
	patch := diff.Patch{}
	updateDoneChan := make(chan struct{})
	go func() {
		for dlr := range updatedChan {
			patch = append(patch, dlr)
		}
		updateDoneChan <- struct{}{}
	}()

	// Create 'concurrency' go routines for processing diffs from foundChan
	transformWg := sync.WaitGroup{}
	for i := uint(0); i < concurrency; i++ {
		transformWg.Add(1)
		go func() {
			runCallback := func(dif diff.Difference) diff.Difference {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered in runCallback", r)
					}
				}()
				return updateCb(dif)
			}

			for dif := range foundChan {
				d1 := runCallback(dif)
				if !d1.IsEmpty() {
					updatedChan <- d1
				}
			}
			transformWg.Done()
		}()
	}

	// The treewalkCallback calls the shouldReplace callback on each node being
	// traversed. If true is returned, the Value is wrapped in a Difference object
	// and sent to foundChan for processing. TreeWalk does not traverse any of
	// children of Values sent to foundChan.
	twcb := func(p types.Path, parent, v types.Value) bool {
		if shouldUpdateCb(p, root, parent, v) {
			p1 := make(types.Path, len(p))
			copy(p1, p)
			foundChan <- diff.Difference{Path: p1, ChangeType: types.DiffChangeModified, OldValue: v}
			return true
		}
		return false
	}

	TreeWalk(vr, types.Path{}, root, twcb)
	close(foundChan)
	transformWg.Wait()
	close(updatedChan)
	<-updateDoneChan

	// If no diffs were generated, then no transforms were made, just return the
	// original root
	if len(patch) == 0 {
		return root
	}

	return diff.Apply(root, patch)
}
