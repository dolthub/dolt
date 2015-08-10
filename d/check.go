package d

import (
	"fmt"

	"github.com/stretchr/testify/assert"
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
	panic(nomsError{fmt.Sprintf(format, args...)})
}
