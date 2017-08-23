package jenkins

import "hash"

type jenkhash uint32

func New() hash.Hash32 {
	var j jenkhash = 0
	return &j
}

func (j *jenkhash) Write(key []byte) (int, error) {
	hash := *j

	for _, b := range key {
		hash += jenkhash(b)
		hash += (hash << 10)
		hash ^= (hash >> 6)
	}

	hash += (hash << 3)
	hash ^= (hash >> 11)
	hash += (hash << 15)

	*j = hash
	return len(key), nil
}

func (j *jenkhash) Reset() {
	*j = 0
}

func (j *jenkhash) Size() int {
	return 4
}

func (j *jenkhash) BlockSize() int {
	return 1
}

func (j *jenkhash) Sum32() uint32 {
	return uint32(*j)
}

func (j *jenkhash) Sum(in []byte) []byte {
	v := j.Sum32()
	return append(in, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
