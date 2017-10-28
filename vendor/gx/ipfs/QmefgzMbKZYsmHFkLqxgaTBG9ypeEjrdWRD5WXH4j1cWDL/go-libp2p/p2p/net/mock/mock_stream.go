package mocknet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
)

// stream implements inet.Stream
type stream struct {
	write     *io.PipeWriter
	read      *io.PipeReader
	conn      *conn
	toDeliver chan *transportObject

	reset  chan struct{}
	close  chan struct{}
	closed chan struct{}

	state error

	protocol protocol.ID
}

var ErrReset error = errors.New("stream reset")
var ErrClosed error = errors.New("stream closed")

type transportObject struct {
	msg         []byte
	arrivalTime time.Time
}

func NewStream(w *io.PipeWriter, r *io.PipeReader) *stream {
	s := &stream{
		read:      r,
		write:     w,
		reset:     make(chan struct{}, 1),
		close:     make(chan struct{}, 1),
		closed:    make(chan struct{}),
		toDeliver: make(chan *transportObject),
	}

	go s.transport()
	return s
}

//  How to handle errors with writes?
func (s *stream) Write(p []byte) (n int, err error) {
	l := s.conn.link
	delay := l.GetLatency() + l.RateLimit(len(p))
	t := time.Now().Add(delay)
	select {
	case <-s.closed: // bail out if we're closing.
		return 0, s.state
	case s.toDeliver <- &transportObject{msg: p, arrivalTime: t}:
	}
	return len(p), nil
}

func (s *stream) Protocol() protocol.ID {
	return s.protocol
}

func (s *stream) SetProtocol(proto protocol.ID) {
	s.protocol = proto
}

func (s *stream) Close() error {
	select {
	case s.close <- struct{}{}:
	default:
	}
	<-s.closed
	if s.state != ErrClosed {
		return s.state
	}
	return nil
}

func (s *stream) Reset() error {
	// Cancel any pending writes.
	s.write.Close()

	select {
	case s.reset <- struct{}{}:
	default:
	}
	<-s.closed
	if s.state != ErrReset {
		return s.state
	}
	return nil
}

func (s *stream) teardown() {
	s.write.Close()

	// at this point, no streams are writing.
	s.conn.removeStream(s)

	// Mark as closed.
	close(s.closed)

	s.conn.net.notifyAll(func(n inet.Notifiee) {
		n.ClosedStream(s.conn.net, s)
	})
}

func (s *stream) Conn() inet.Conn {
	return s.conn
}

func (s *stream) SetDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) SetReadDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) SetWriteDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (s *stream) Read(b []byte) (int, error) {
	return s.read.Read(b)
}

// transport will grab message arrival times, wait until that time, and
// then write the message out when it is scheduled to arrive
func (s *stream) transport() {
	defer s.teardown()

	bufsize := 256
	buf := new(bytes.Buffer)
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	// cleanup
	defer timer.Stop()

	// writeBuf writes the contents of buf through to the s.Writer.
	// done only when arrival time makes sense.
	drainBuf := func() {
		if buf.Len() > 0 {
			_, err := s.write.Write(buf.Bytes())
			if err != nil {
				return
			}
			buf.Reset()
		}
	}

	// deliverOrWait is a helper func that processes
	// an incoming packet. it waits until the arrival time,
	// and then writes things out.
	deliverOrWait := func(o *transportObject) {
		buffered := len(o.msg) + buf.Len()

		// Yes, we can end up extending a timer multiple times if we
		// keep on making small writes but that shouldn't be too much of an
		// issue. Fixing that would be painful.
		if !timer.Stop() {
			// FIXME: So, we *shouldn't* need to do this but we hang
			// here if we don't... Go bug?
			select {
			case <-timer.C:
			default:
			}
		}
		delay := o.arrivalTime.Sub(time.Now())
		if delay >= 0 {
			timer.Reset(delay)
		} else {
			timer.Reset(0)
		}

		if buffered >= bufsize {
			select {
			case <-timer.C:
			case <-s.reset:
				s.reset <- struct{}{}
				return
			}
			drainBuf()
			// write this message.
			_, err := s.write.Write(o.msg)
			if err != nil {
				log.Error("mock_stream", err)
			}
		} else {
			buf.Write(o.msg)
		}
	}

	for {
		// Reset takes precedent.
		select {
		case <-s.reset:
			s.state = ErrReset
			s.read.CloseWithError(ErrReset)
			return
		default:
		}

		select {
		case <-s.reset:
			s.state = ErrReset
			s.read.CloseWithError(ErrReset)
			return
		case <-s.close:
			s.state = ErrClosed
			drainBuf()
			return
		case o := <-s.toDeliver:
			deliverOrWait(o)
		case <-timer.C: // ok, due to write it out.
			drainBuf()
		}
	}
}
