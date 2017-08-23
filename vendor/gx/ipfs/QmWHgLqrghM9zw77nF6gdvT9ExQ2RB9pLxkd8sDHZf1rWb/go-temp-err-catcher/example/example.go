package main

import (
	"fmt"

	tec "gx/ipfs/QmWHgLqrghM9zw77nF6gdvT9ExQ2RB9pLxkd8sDHZf1rWb/go-temp-err-catcher"
)

var (
	ErrTemp  = tec.ErrTemporary{fmt.Errorf("ErrTemp")}
	ErrSkip  = fmt.Errorf("ErrSkip")
	ErrOther = fmt.Errorf("ErrOther")
)

func main() {
	var normal tec.TempErrCatcher
	var skipper tec.TempErrCatcher
	skipper.IsTemp = func(e error) bool {
		return e == ErrSkip
	}

	fmt.Println("trying normal (uses Temporary interface)")
	tryTec(normal)
	fmt.Println("")
	fmt.Println("trying skipper (uses our IsTemp function)")
	tryTec(skipper)
}

func tryTec(c tec.TempErrCatcher) {
	errs := []error{
		ErrTemp,
		ErrSkip,
		ErrOther,
		ErrTemp,
		ErrSkip,
		ErrOther,
	}

	for _, e := range errs {
		if c.IsTemporary(e) {
			fmt.Printf("\tIsTemporary: true  - skipped     %s\n", e)
			continue
		}

		fmt.Printf("\tIsTemporary: false - not skipped %s\n", e)
	}
}
