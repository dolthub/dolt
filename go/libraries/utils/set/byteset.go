package set

type ByteSet struct {
	bytes map[byte]interface{}
}

func NewByteSet(bytes []byte) *ByteSet {
	s := &ByteSet{make(map[byte]interface{}, len(bytes))}

	for _, b := range bytes {
		s.bytes[b] = emptyInstance
	}

	return s
}

func (bs *ByteSet) Contains(b byte) bool {
	_, present := bs.bytes[b]
	return present
}

func (bs *ByteSet) ContainsAll(bytes []byte) bool {
	for _, b := range bytes {
		if _, present := bs.bytes[b]; !present {
			return false
		}
	}

	return true
}
