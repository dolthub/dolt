package types

type blobLeafSequence struct {
	data []byte
	vr   ValueReader
}

func newBlobLeafSequence(vr ValueReader, data []byte) indexedSequence {
	return blobLeafSequence{data, vr}
}

func (bl blobLeafSequence) getItem(idx int) sequenceItem {
	return bl.data[idx]
}

func (bl blobLeafSequence) seqLen() int {
	return len(bl.data)
}

func (bl blobLeafSequence) numLeaves() uint64 {
	return uint64(len(bl.data))
}

func (bl blobLeafSequence) getOffset(idx int) uint64 {
	return uint64(idx)
}

func (bl blobLeafSequence) Chunks() []Ref {
	return []Ref{}
}

func (bl blobLeafSequence) Type() *Type {
	return BlobType
}

func (bl blobLeafSequence) valueReader() ValueReader {
	return bl.vr
}
