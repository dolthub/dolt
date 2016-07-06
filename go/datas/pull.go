// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"container/heap"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// Pull objects that descends from sourceRef from srcDB to sinkDB. sinkHeadRef should point to a Commit (in sinkDB) that's an ancestor of sourceRef. This allows the algorithm to figure out which portions of data are already present in sinkDB and skip copying them.
// TODO: Figure out how to add concurrency.
func Pull(srcDB, sinkDB Database, sourceRef, sinkHeadRef types.Ref, concurrency int) {
	srcQ, sinkQ := types.RefHeap{sourceRef}, types.RefHeap{sinkHeadRef}
	heap.Init(&srcQ)
	heap.Init(&sinkQ)

	// We generally expect that sourceRef descends from sinkHeadRef, so that walking down from sinkHeadRef yields useful hints. If it's not even in the srcDB, then just clear out sinkQ right now and don't bother.
	if !srcDB.has(sinkHeadRef.TargetHash()) {
		heap.Pop(&sinkQ)
	}

	// Since we expect sourceRef to descend from sinkHeadRef, we assume srcDB has a superset of the data in sinkDB. There are some cases where, logically, the code wants to read data it knows to be in sinkDB. In this case, it doesn't actually matter which Database the data comes from, so as an optimization we use whichever is a LocalDatabase -- if either is.
	mostLocalDB := srcDB
	if _, ok := sinkDB.(*LocalDatabase); ok {
		mostLocalDB = sinkDB
	}
	// traverseWorker below takes refs off of {src,sink,com}Chan, processes them to figure out what reachable refs should be traversed, and then sends the results to {srcRes,sinkRes,comRes}Chan.
	// sending to (or closing) the 'done' channel causes traverseWorkers to exit.
	srcChan := make(chan types.Ref)
	sinkChan := make(chan types.Ref)
	comChan := make(chan types.Ref)
	srcResChan := make(chan traverseResult)
	sinkResChan := make(chan traverseResult)
	comResChan := make(chan traverseResult)
	done := make(chan struct{})

	workerWg := &sync.WaitGroup{}
	defer func() {
		close(done)
		workerWg.Wait()

		close(srcChan)
		close(sinkChan)
		close(comChan)
		close(srcResChan)
		close(sinkResChan)
		close(comResChan)
	}()
	traverseWorker := func() {
		workerWg.Add(1)
		go func() {
			for {
				select {
				case srcRef := <-srcChan:
					srcResChan <- traverseSource(srcRef, srcDB, sinkDB)
				case sinkRef := <-sinkChan:
					sinkResChan <- traverseSink(sinkRef, mostLocalDB)
				case comRef := <-comChan:
					comResChan <- traverseCommon(comRef, sinkHeadRef, mostLocalDB)
				case <-done:
					workerWg.Done()
					return
				}
			}
		}()
	}
	for i := 0; i < concurrency; i++ {
		traverseWorker()
	}

	// hc and reachableChunks aren't goroutine-safe, so only write them here.
	hc := hintCache{}
	reachableChunks := hashSet{}
	for !srcQ.Empty() {
		srcRefs, sinkRefs, comRefs := planWork(&srcQ, &sinkQ)
		srcWork, sinkWork, comWork := len(srcRefs), len(sinkRefs), len(comRefs)

		// These goroutines send work to traverseWorkers, blocking when all are busy. They self-terminate when they've sent all they have.
		go sendWork(srcChan, srcRefs)
		go sendWork(sinkChan, sinkRefs)
		go sendWork(comChan, comRefs)
		//  Don't use srcRefs, sinkRefs, or comRefs after this point. The goroutines above own them.

		for srcWork+sinkWork+comWork > 0 {
			select {
			case res := <-srcResChan:
				for _, reachable := range res.reachables {
					heap.Push(&srcQ, reachable)
					reachableChunks.Insert(reachable.TargetHash())
				}
				if !res.readHash.IsEmpty() {
					reachableChunks.Remove(res.readHash)
				}
				srcWork--
			case res := <-sinkResChan:
				for _, reachable := range res.reachables {
					heap.Push(&sinkQ, reachable)
					hc[reachable.TargetHash()] = res.readHash
				}
				sinkWork--
			case res := <-comResChan:
				isHeadOfSink := res.readHash == sinkHeadRef.TargetHash()
				for _, reachable := range res.reachables {
					heap.Push(&sinkQ, reachable)
					if !isHeadOfSink {
						heap.Push(&srcQ, reachable)
					}
					hc[reachable.TargetHash()] = res.readHash
				}
				comWork--
			}
		}
	}
	hints := types.Hints{}
	for hash := range reachableChunks {
		if hint, present := hc[hash]; present {
			hints[hint] = struct{}{}
		}
	}
	sinkDB.batchStore().AddHints(hints)
}

type traverseResult struct {
	readHash   hash.Hash
	reachables types.RefSlice
}

// planWork deals with three possible situations:
// - head of srcQ is higher than head of sinkQ
// - head of sinkQ is higher than head of srcQ
// - both heads are at the same height
//
// As we build up lists of refs to be processed in parallel, we need to avoid blowing past potential common refs. When processing a given Ref, we enumerate Refs of all Chunks that are directly reachable, which must _by definition_ be shorter than the given Ref. This means that, for example, if the queues are the same height we know that nothing can happen that will put more Refs of that height on either queue. In general, if you look at the height of the Ref at the head of a queue, you know that all Refs of that height in the current graph under consideration are already in the queue. Conversely, for any height less than that of the head of the queue, it's possible that Refs of that height remain to be discovered. Given this, we can figure out which Refs are safe to pull off the 'taller' queue in the cases where the heights of the two queues are not equal.
// If one queue is 'taller' than the other, it's clear that we can process all refs from the taller queue with height greater than the height of the 'shorter' queue. We should also be able to process refs from the taller queue that are of the same height as the shorter queue, as long as we also check to see if they're common to both queues. It is not safe, however, to pull unique items off the shorter queue at this point. It's possible that, in processing some of the Refs from the taller queue, that these Refs will be discovered to be common after all.
func planWork(srcQ, sinkQ *types.RefHeap) (srcRefs, sinkRefs, comRefs types.RefSlice) {
	srcHt, sinkHt := headHeight(srcQ), headHeight(sinkQ)
	if srcHt > sinkHt {
		srcRefs, comRefs = burnDown(srcQ, sinkQ, srcHt, sinkHt)
		return
	}
	if sinkHt > srcHt {
		sinkRefs, comRefs = burnDown(sinkQ, srcQ, sinkHt, srcHt)
		return
	}
	d.Chk.True(srcHt == sinkHt)
	srcRefs, comRefs = burnDown(srcQ, sinkQ, srcHt, sinkHt)
	sinkRefs, _ = burnDown(sinkQ, srcQ, sinkHt, srcHt)
	return
}

func burnDown(taller, shorter *types.RefHeap, tall, short uint64) (tallRefs, comRefs types.RefSlice) {
	for ht := tall; ht > short; ht = headHeight(taller) {
		tallRefs = append(tallRefs, heap.Pop(taller).(types.Ref))
	}
	for shortIdx := 0; !taller.Empty() && headHeight(taller) == short; {
		tallRef := heap.Pop(taller).(types.Ref)
		shortPeek := shorter.PeekAt(shortIdx)
		if types.HeapOrder(tallRef, shortPeek) {
			tallRefs = append(tallRefs, tallRef)
			continue
		}
		if types.HeapOrder(shortPeek, tallRef) {
			shortIdx++
			continue
		}
		d.Chk.True(tallRef.Equals(shortPeek))
		heap.Remove(shorter, shortIdx)
		comRefs = append(comRefs, tallRef)
	}
	return
}

func headHeight(h *types.RefHeap) uint64 {
	return h.Peek().Height()
}

func sendWork(ch chan<- types.Ref, refs types.RefSlice) {
	for _, r := range refs {
		ch <- r
	}
}

type hintCache map[hash.Hash]hash.Hash

func traverseSource(srcRef types.Ref, srcDB, sinkDB Database) traverseResult {
	h := srcRef.TargetHash()
	if !sinkDB.has(h) {
		srcBS := srcDB.batchStore()
		c := srcBS.Get(h)
		v := types.DecodeValue(c, srcDB)
		d.Chk.True(v != nil, "Expected decoded chunk to be non-nil.")
		sinkDB.batchStore().SchedulePut(c, srcRef.Height(), types.Hints{})
		return traverseResult{h, v.Chunks()}
	}
	return traverseResult{}
}

func traverseSink(sinkRef types.Ref, db Database) traverseResult {
	if sinkRef.Height() > 1 {
		return traverseResult{sinkRef.TargetHash(), sinkRef.TargetValue(db).Chunks()}
	}
	return traverseResult{}
}

func traverseCommon(comRef, sinkHead types.Ref, db Database) traverseResult {
	if comRef.Height() > 1 && comRef.Type().Equals(refOfCommitType) {
		commit := comRef.TargetValue(db).(types.Struct)
		// We don't want to traverse the parents of sinkHead, but we still want to traverse its Value on the sinkDB side. We also still want to traverse all children, in both the srcDB and sinkDB, of any common Commit that is not at the Head of sinkDB.
		exclusionSet := types.NewSet()
		if comRef.Equals(sinkHead) {
			exclusionSet = commit.Get(ParentsField).(types.Set)
		}
		chunks := types.RefSlice(commit.Chunks())
		for i := 0; i < len(chunks); {
			if exclusionSet.Has(chunks[i]) {
				end := len(chunks) - 1
				chunks.Swap(i, end)
				chunks = chunks[:end]
				continue
			}
			i++
		}
		return traverseResult{comRef.TargetHash(), chunks}
	}
	return traverseResult{}
}
