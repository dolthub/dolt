package types

type sequenceCursor interface {
	getParent() sequenceCursor
	clone() sequenceCursor
	current() sequenceItem
	advance() bool
	retreat() bool
	indexInChunk() int
}

func cursorGetMaxNPrevItems(seq sequenceCursor, n int) (prev []sequenceItem) {
	retreater := seq.clone()

	for i := 0; i < n && retreater.retreat(); i++ {
		prev = append(prev, retreater.current())
	}
	for i := 0; i < len(prev)/2; i++ {
		t := prev[i]
		prev[i] = prev[len(prev)-i-1]
		prev[len(prev)-i-1] = t
	}

	return
}
