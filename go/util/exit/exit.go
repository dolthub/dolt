// Package exit provides a mockable implementation of os.Exit.
// That's all!
package exit

import (
	"os"
)

var Exit = func(code int) {
	os.Exit(code)
}

func Fail() {
	Exit(1)
}

func Success() {
	Exit(0)
}
