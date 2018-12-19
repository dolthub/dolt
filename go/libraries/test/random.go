package test

import "math/rand"

const (
	// ShouldNeverHappen is only seen when the impossible happens.
	ShouldNeverHappen = "http://www.nooooooooooooooo.com"
)

// RandomData returns a slice of a given size filled with random data
func RandomData(size int) []byte {
	randBytes := make([]byte, size)
	filled := 0
	for filled < size {
		n, err := rand.Read(randBytes[filled:])

		if err != nil {
			panic(ShouldNeverHappen)
		}

		filled += n
	}

	return randBytes
}
