// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package d

import (
	"fmt"

	"github.com/attic-labs/testify/assert"
)

var (
	Chk = assert.New(&panicker{})

	// Exp provides the same API as Chk, but the resulting panics can be caught by d.Try()
	Exp = assert.New(&recoverablePanicker{})
)

type panicker struct {
}

func (s panicker) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type recoverablePanicker struct {
}

func (s recoverablePanicker) Errorf(format string, args ...interface{}) {
	panic(UsageError{fmt.Sprintf(format, args...)})
}
