package ase

// KVPCollBuilder is used to build a KVPCollection.  It creates two buffers which it fills with KVPs.  When a buffer
// is filled the target buffer is changed for subsequent adds.  New buffers can be added to the builder so that
// buffers of other KVPCollections can be reused.
type KVPCollBuilder struct {
	filled     []KVPSlice
	toFill     []KVPSlice
	currSl     KVPSlice
	currSlSize int
	currIdx    int
	numItems   int64
	buffSize   int
}

// NewKVPCollBuilder creates a builder which can be used to
func NewKVPCollBuilder(buffSize int) *KVPCollBuilder {
	buffs := []KVPSlice{make(KVPSlice, buffSize)}
	currSl := make(KVPSlice, buffSize)

	return &KVPCollBuilder{nil, buffs, currSl, buffSize, 0, 0, buffSize}
}

// AddBuffer adds a buffer of KVPs that can be filled.
func (cb *KVPCollBuilder) AddBuffer(buff KVPSlice) {
	if cap(buff) != cb.buffSize {
		panic("All buffers should be created with the same capacity.")
	}

	cb.toFill = append(cb.toFill, buff[:cap(buff)])
}

// AddKVP adds a KVP to the current buffer
func (cb *KVPCollBuilder) AddKVP(kvp KVP) {
	cb.currSl[cb.currIdx] = kvp

	cb.currIdx++

	if cb.currIdx == cb.currSlSize {
		cb.doneWithCurrBuff()
	}
}

func (cb *KVPCollBuilder) doneWithCurrBuff() {
	cb.numItems += int64(cb.currIdx)
	cb.filled = append(cb.filled, cb.currSl[:cb.currIdx])

	cb.currIdx = 0

	if len(cb.toFill) > 0 {
		cb.currSl = cb.toFill[0]
		cb.currSlSize = len(cb.currSl)
		cb.toFill = cb.toFill[1:]
	} else {
		cb.currSl = nil
		cb.currSlSize = 0
	}
}

// MoveRemaining takes a KVPCollItr and moves all the KVPs that still need to be iterated over and moves them
// into the internal KVP buffers.
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
		cb.numItems += int64(len(currSl))
	}
}

// Build takes all the filled and partially filled buffers and creates a new KVPCollection from them.
func (cb *KVPCollBuilder) Build() *KVPCollection {
	if cb.currIdx != 0 {
		cb.doneWithCurrBuff()
	}

	return &KVPCollection{cb.buffSize, len(cb.filled), cb.numItems, cb.filled}
}
