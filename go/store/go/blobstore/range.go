package blobstore

// BlobRange represents a segment of a blob of data.  Offset is the beginning of
// the range and Length is the size.  If Length is 0 that means all data beyond
// offset will be read. Lengths cannot be negative.  Negative offsets indicate
// distance from the end of the end of the blob.
type BlobRange struct {
	offset int64
	length int64
}

// NewBlobRange creates a BlobRange with a given offset and length
func NewBlobRange(offset, length int64) BlobRange {
	if length < 0 {
		panic("BlobRanges cannot have 0 length")
	}

	return BlobRange{offset, length}
}

// IsAllRange is true if it represents the entire blob from index 0 to the end
// and false if it is any subset of the data
func (br BlobRange) isAllRange() bool {
	return br.offset == 0 && br.length == 0
}

// PositiveRange returns a BlobRange which represents the same range but with
// negative offsets replaced with actual values
func (br BlobRange) positiveRange(size int64) BlobRange {
	offset := br.offset
	length := br.length

	if offset < 0 {
		offset = size + offset
	}

	if offset+length > size || length == 0 {
		length = size - offset
	}

	return BlobRange{offset, length}
}

// AllRange is a BlobRange instance covering all values
var AllRange = NewBlobRange(0, 0)
