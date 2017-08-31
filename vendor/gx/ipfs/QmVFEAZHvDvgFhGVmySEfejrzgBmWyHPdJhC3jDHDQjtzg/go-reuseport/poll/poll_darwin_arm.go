// +build darwin,arm64

package poll

import (
	"context"
	"syscall"
	"time"
)

type Poller struct {
	kqfd  int
	event syscall.Kevent_t
}

func New(fd int) (p *Poller, err error) {
	p = &Poller{}

	p.kqfd, err = syscall.Kqueue()
	if p.kqfd == -1 || err != nil {
		return nil, err
	}

	p.event = syscall.Kevent_t{
		Ident:  uint32(fd),
		Filter: syscall.EVFILT_WRITE,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_ONESHOT,
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}
	return p, nil
}

func (p *Poller) Close() error {
	return syscall.Close(p.kqfd)
}

func (p *Poller) WaitWriteCtx(ctx context.Context) error {
	deadline, _ := ctx.Deadline()

	// setup timeout
	var timeout *syscall.Timespec
	if !deadline.IsZero() {
		d := deadline.Sub(time.Now())
		t := syscall.NsecToTimespec(d.Nanoseconds())
		timeout = &t
	}

	// wait on kevent
	events := make([]syscall.Kevent_t, 1)
	n, err := syscall.Kevent(p.kqfd, []syscall.Kevent_t{p.event}, events, timeout)
	if err != nil {
		return err
	}

	if n < 1 {
		return errTimeout
	}
	return nil
}
