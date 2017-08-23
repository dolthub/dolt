package tcp

import (
	"net"
	"syscall"
	"testing"
)

type netTimeoutErr struct {
	timeout bool
}

func (e netTimeoutErr) Error() string {
	return ""
}

func (e netTimeoutErr) Timeout() bool {
	return e.timeout
}

func (e netTimeoutErr) Temporary() bool {
	panic("not checked")
}

func TestReuseError(t *testing.T) {
	var nte1 net.Error = &netTimeoutErr{true}
	var nte2 net.Error = &netTimeoutErr{false}

	cases := map[error]bool{
		nil:                   false,
		syscall.EADDRINUSE:    true,
		syscall.EADDRNOTAVAIL: true,
		syscall.ECONNREFUSED:  false,

		nte1: false,
		nte2: true, // this ones a little weird... we should check neterror.Temporary() too

		// test 'default' to true
		syscall.EBUSY: true,
	}

	for k, v := range cases {
		if ReuseErrShouldRetry(k) != v {
			t.Fatalf("expected %t for %#v", v, k)
		}
	}

}
