package remotestorage

import "github.com/attic-labs/noms/go/hash"

func HashesToSlices(hashes []hash.Hash) [][]byte {
	slices := make([][]byte, len(hashes))

	for i, h := range hashes {
		tmp := h
		slices[i] = tmp[:]
	}

	return slices
}

func HashSetToSlices(hashes hash.HashSet) ([]hash.Hash, [][]byte) {
	hashSl := make([]hash.Hash, len(hashes))
	bytesSl := make([][]byte, len(hashes))

	i := 0
	for h := range hashes {
		tmp := h
		hashSl[i] = tmp
		bytesSl[i] = tmp[:]
		i++
	}

	return hashSl, bytesSl
}

func ParseByteSlices(byteSlices [][]byte) (hash.HashSet, map[hash.Hash]int) {
	hs := make(hash.HashSet)
	hashToIndex := make(map[hash.Hash]int)

	for i, byteSl := range byteSlices {
		h := hash.New(byteSl)
		hs[h] = struct{}{}
		hashToIndex[h] = i
	}

	return hs, hashToIndex
}
