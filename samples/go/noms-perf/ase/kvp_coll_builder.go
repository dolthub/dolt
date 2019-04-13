package ase

import "sort"

type KVPCollBuilder struct {
	filled     []KVPSlice
	toFill     []KVPSlice
	currSl     KVPSlice
	currSlSize int
	currIdx    int
	numItems   int
	buffSize   int
}

func NewKVPCollBuilder(buffSize int) *KVPCollBuilder {
	buffs := []KVPSlice{make(KVPSlice, buffSize)}
	currSl := make(KVPSlice, buffSize)

	return &KVPCollBuilder{nil, buffs, currSl, buffSize, 0, 0, buffSize}
}

func (cb *KVPCollBuilder) AddBuffer(buff KVPSlice) {
	if cap(buff) != cb.buffSize {
		panic("All buffers should be created with the same capacity.")
	}

	cb.toFill = append(cb.toFill, buff[:cap(buff)])

	sort.Slice(cb.toFill, func(i, j int) bool {
		return len(cb.toFill[i]) < len(cb.toFill[j])
	})
}

func (cb *KVPCollBuilder) AddKVP(kvp KVP) {
	cb.currSl[cb.currIdx] = kvp

	cb.currIdx++

	if cb.currIdx == cb.currSlSize {
		cb.doneWithCurrBuff()
	}
}

func (cb *KVPCollBuilder) doneWithCurrBuff() {
	cb.numItems += cb.currIdx
	cb.filled = append(cb.filled, cb.currSl[:cb.currIdx])

	cb.currIdx = 0
	cb.currSl = cb.toFill[0]
	cb.currSlSize = len(cb.currSl)
	cb.toFill = cb.toFill[1:]
}

func (cb *KVPCollBuilder) MoveRemaining(itr *KVPCollItr) {
	remInCurr := itr.currSlSize - itr.idx
	remInDest := cb.currSlSize - cb.currIdx

	if remInDest < remInCurr {
		cb.doneWithCurrBuff()
	}

	copy(cb.currSl[cb.currIdx:], itr.currSl[itr.idx:])
	cb.currIdx += remInCurr
	cb.doneWithCurrBuff()

	for itr.slIdx++; itr.slIdx < itr.coll.numSlices; itr.slIdx++ {
		currSl := itr.coll.slices[itr.slIdx]
		cb.filled = append(cb.filled, currSl)
		cb.numItems += len(currSl)
	}
}

func (cb *KVPCollBuilder) Build() *KVPCollection {
	if cb.currIdx != 0 {
		cb.doneWithCurrBuff()
	}

	maxSize := len(cb.filled[0])
	for i := 1; i < len(cb.filled); i++ {
		currSize := len(cb.filled[i])
		if currSize > maxSize {
			maxSize = currSize
		}
	}

	return &KVPCollection{maxSize, len(cb.filled), cb.numItems, cb.filled}
}
