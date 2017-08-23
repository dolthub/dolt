package tour

import "fmt"

// returns a partially applied function.
//
// It's designed to make it easy to re-order chapters with minimal fuss.
//
// eg.
// 		Intro := Chapter(1)
// 		ID("1.1") == Intro(1) == Chapter(1)(1)
func Chapter(number int) func(topic int) ID {
	return func(topic int) ID {
		return ID(fmt.Sprintf("%d.%d", number, topic))
	}
}
