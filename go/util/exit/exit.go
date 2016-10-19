// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

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
