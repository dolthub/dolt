// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"container/heap"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// Pull objects that descends from sourceRef from srcDB to sinkDB. sinkHeadRef should point to a Commit (in sinkDB) that's an ancestor of sourceRef. This allows the algorithm to figure out which portions of data are already present in sinkDB and skip copying them.
// TODO: Figure out how to add concurrency.
func Pull(srcDB, sinkDB Database, sourceRef, sinkHeadRef types.Ref) {
	srcQ, sinkQ := types.RefHeap{sourceRef}, types.RefHeap{sinkHeadRef}
	heap.Init(&srcQ)
	heap.Init(&sinkQ)

	// We generally expect that sourceRef descends from sinkHeadRef, so that walking down from sinkHeadRef yields useful hints. If it's not even in the srcDB, then just clear out sinkQ right now and don't bother.
	if !srcDB.has(sinkHeadRef.TargetHash()) {
		heap.Pop(&sinkQ)
	}

	hc := hintCache{}
	reachableChunks := hashSet{}

	// Since we expect sourceRef to descend from sinkHeadRef, we assume srcDB has a superset of the data in sinkDB. There are some cases where, logically, the code wants to read data it knows to be in sinkDB. In this case, it doesn't actually matter which Database the data comes from, so as an optimization we use whichever is a LocalDatabase -- if either is.
	mostLocalDB := srcDB
	if _, ok := sinkDB.(*LocalDatabase); ok {
		mostLocalDB = sinkDB
	}

	for !srcQ.Empty() {
		srcRef := srcQ[0]

		// If the head of one Q is "higher" than the other, traverse it and then loop again. "HigherThan" sorts first by greater ref-height, then orders Refs by TargetHash.
		if sinkQ.Empty() || types.HigherThan(srcRef, sinkQ[0]) {
			traverseSource(&srcQ, srcDB, sinkDB, reachableChunks)
			continue
		}
		// Either the head of sinkQ is higher, or the heads of both queues are equal.
		if types.HigherThan(sinkQ[0], srcRef) {
			traverseSink(&sinkQ, mostLocalDB, hc)
			continue
		}

		// The heads of both Qs are the same.
		d.Chk.True(!sinkQ.Empty(), "The heads should be the same, but sinkQ is empty!")
		traverseCommon(sinkHeadRef, &sinkQ, &srcQ, mostLocalDB, hc)
	}
	hints := types.Hints{}
	for hash := range reachableChunks {
		if hint, present := hc[hash]; present {
			hints[hint] = struct{}{}
		}
	}
	sinkDB.batchStore().AddHints(hints)
}

type hintCache map[hash.Hash]hash.Hash

func traverseSource(srcQ *types.RefHeap, src Database, sinkDB Database, reachableChunks hashSet) {
	srcRef := heap.Pop(srcQ).(types.Ref)
	h := srcRef.TargetHash()
	if !sinkDB.has(h) {
		srcBS := src.batchStore()
		c := srcBS.Get(h)
		v := types.DecodeValue(c, src)
		d.Chk.True(v != nil, "Expected decoded chunk to be non-nil.")
		for _, reachable := range v.Chunks() {
			heap.Push(srcQ, reachable)
			reachableChunks.Insert(reachable.TargetHash())
		}
		sinkDB.batchStore().SchedulePut(c, srcRef.Height(), types.Hints{})
		delete(reachableChunks, h)
	}
}

func traverseSink(sinkQ *types.RefHeap, db Database, hc hintCache) {
	sinkRef := heap.Pop(sinkQ).(types.Ref)
	if sinkRef.Height() > 1 {
		h := sinkRef.TargetHash()
		for _, reachable := range sinkRef.TargetValue(db).Chunks() {
			heap.Push(sinkQ, reachable)
			hc[reachable.TargetHash()] = h
		}
	}
}

func traverseCommon(sinkHead types.Ref, sinkQ, srcQ *types.RefHeap, db Database, hc hintCache) {
	comRef, sinkRef := heap.Pop(srcQ).(types.Ref), heap.Pop(sinkQ).(types.Ref)
	d.Chk.True(comRef.Equals(sinkRef), "traverseCommon expects refs to be equal: %s != %s", comRef.TargetHash(), sinkRef.TargetHash())
	if comRef.Height() == 1 {
		return
	}
	if comRef.Type().Equals(refOfCommitType) {
		commit := comRef.TargetValue(db).(types.Struct)
		// We don't want to traverse the parents of sinkHead, but we still want to traverse its Value on the sinkDB side. We also still want to traverse all children, in both the srcDB and sinkDB, of any common Commit that is not at the Head of sinkDB.
		isHeadOfSink := comRef.Equals(sinkHead)
		exclusionSet := types.NewSet()
		if isHeadOfSink {
			exclusionSet = commit.Get(ParentsField).(types.Set)
		}
		commitHash := comRef.TargetHash()
		for _, reachable := range commit.Chunks() {
			if !exclusionSet.Has(reachable) {
				heap.Push(sinkQ, reachable)
				if !isHeadOfSink {
					heap.Push(srcQ, reachable)
				}
				hc[reachable.TargetHash()] = commitHash
			}
		}
	}
}
