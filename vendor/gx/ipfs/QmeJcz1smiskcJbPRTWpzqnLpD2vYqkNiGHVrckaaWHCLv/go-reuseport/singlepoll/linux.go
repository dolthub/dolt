// +build linux

package singlepoll

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"gx/ipfs/QmSGRM5Udmy1jsFBr1Cawez7Lt7LZ3ZKA23GGVEsiEW6F3/eventfd"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var (
	ErrUnsupportedMode error = errors.New("only 'w' and 'r' modes are supported on this arch")
)

var (
	initOnce sync.Once
	workChan chan interface{}
	log      logging.EventLogger = logging.Logger("reuseport-poll")
)

type addPoll struct {
	fd     int
	events uint32
	ctx    context.Context
	wakeUp chan<- error
}

type ctxDone struct {
	fd int
}

func PollPark(reqctx context.Context, fd int, mode string) error {
	initOnce.Do(func() {
		workChan = make(chan interface{}, 128)
		go worker()
	})

	events := uint32(0)
	for _, c := range mode {
		switch c {
		case 'w':
			events |= syscall.EPOLLOUT
		case 'r':
			events |= syscall.EPOLLIN
		default:
			return ErrUnsupportedMode
		}
	}

	wakeUp := make(chan error)
	workChan <- addPoll{
		fd:     fd,
		events: events,
		ctx:    reqctx,
		wakeUp: wakeUp,
	}

	return <-wakeUp
}

func criticalError(msg string, err error) {
	log.Errorf("%s: %s.", msg, err.Error())
	log.Errorf("This is critical error, please report it at https://github.com/jbenet/go-reuseport/issues/new")
	log.Errorf("Bailing out. You are on your own. Good luck.")

	for {
		select {
		case <-backgroundctx.Done():
			return
		case unit := <-workChan:
			switch u := unit.(type) {
			case addPoll:
				u.wakeUp <- err
			default:
			}
		}
	}
}

func worker() {
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		criticalError("EpollCreate1(0) failed", err)
	}
	evfd, err := eventfd.New()
	if err != nil {
		criticalError("eventfd.New() failed", err)
	}

	pool := make(map[int]addPoll)

	{
		event := syscall.EpollEvent{
			Events: syscall.EPOLLIN,
			Fd:     int32(evfd.Fd()),
		}
		syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, evfd.Fd(), &event)
	}
	go poller(epfd, evfd)

	remove := func(fd int) *addPoll {
		unit, ok := pool[fd]
		if ok {
			syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, unit.fd, nil)
			delete(pool, fd)
			close(unit.wakeUp)
		}
		return &unit
	}
	for {
		select {
		case <-backgroundctx.Done():
			evfd.WriteEvents(1)
			return
		case unit := <-workChan:
			switch u := unit.(type) {
			case addPoll:
				event := syscall.EpollEvent{
					Events: u.events | syscall.EPOLLONESHOT,
					Fd:     int32(u.fd),
				}

				// Make copies for *I* before we add it to Epoll group
				wrapWakeUp := make(chan error)
				wakeUp := u.wakeUp
				u.wakeUp = wrapWakeUp

				if _, ok := pool[u.fd]; ok {
					panic("duplicate fd") // safe guard against bad close calls
				}
				pool[u.fd] = u

				err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, u.fd, &event)
				if err != nil {
					delete(pool, u.fd)
					u.wakeUp <- err
				}

				// *I*
				reqCtx := u.ctx
				fd := u.fd

				go func() {
					select {
					case err := <-wrapWakeUp:
						wakeUp <- err
					case <-reqCtx.Done():
						workChan <- ctxDone{
							fd: fd,
						}
						<-wrapWakeUp
						wakeUp <- reqCtx.Err()
					}
				}()

			case []syscall.EpollEvent:
				for _, event := range u {
					remove(int(event.Fd))
				}
			case ctxDone:
				remove(u.fd)
			}
		}
	}

}

func poller(epfd int, evfd *eventfd.EventFD) {
	for {
		// do not reuse the array as we will be passing it over channel
		// 128 is quite arbitrary
		// to small and number of EpollWait calls would increase
		// to big and GC overhead increases
		events := make([]syscall.EpollEvent, 128)
		n, err := syscall.EpollWait(epfd, events, -1)

		switch err {
		case nil:
			// everything is great
		case syscall.EINTR:
			// ignore
			continue
		default:
			// log
			log.Errorf("EpollWait returned error: %s. Continuing.", err.Error())
			continue
		}

		for i := 0; i < n; i++ {
			if int(events[i].Fd) == evfd.Fd() {
				syscall.Close(epfd)
				evfd.Close()
				return
			}
		}
		workChan <- events[:n]
	}
}
