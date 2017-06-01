// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package exit provides a mockable implementation of os.Exit.
// That's all!
package exit

import (
	"os"
)

var def = func(code int) {
	os.Exit(code)
}

var Exit = def

// Sets the implementation of Exit() to the default.
func Reset() {
	Exit = def
}

// Exits with a failure status.
func Fail() {
	Exit(1)
}

// Exits with a success status.
func Success() {
	Exit(0)
}
