package dbg

import (
	"fmt"

	"github.com/stretchr/testify/assert"
)

var (
	Chk = assert.New(&panicker{})
)

type panicker struct {
}

func (s panicker) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
