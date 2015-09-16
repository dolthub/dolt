package d

import (
	"fmt"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

var (
	Chk = &Assertions{
		assert.New(&panicker{}),
	}
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

type Assertions struct {
	*assert.Assertions
}

func (a *Assertions) NoChannelError(err <-chan interface{}) {
	select {
	case error, _ := <-err:
		a.NotNil(error)
	default:
		break
	}
}
