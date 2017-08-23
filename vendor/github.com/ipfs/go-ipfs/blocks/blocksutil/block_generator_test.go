package blocksutil

import "testing"

func TestBlocksAreDifferent(t *testing.T) {
	gen := NewBlockGenerator()

	blocks := gen.Blocks(100)

	for i, block1 := range blocks {
		for j, block2 := range blocks {
			if i != j {
				if block1.String() == block2.String() {
					t.Error("Found duplicate blocks")
				}
			}
		}
	}
}
