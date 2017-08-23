package multistream

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

type Multistream interface {
	io.ReadWriteCloser
}

func NewMSSelect(c io.ReadWriteCloser, proto string) Multistream {
	return &lazyConn{
		protos: []string{ProtocolID, proto},
		con:    c,
	}
}

func NewMultistream(c io.ReadWriteCloser, proto string) Multistream {
	return &lazyConn{
		protos: []string{proto},
		con:    c,
	}
}

type lazyConn struct {
	rhandshake bool // only accessed by 'Read' should not call read async

	rhlock sync.Mutex
	rhsync bool //protected by mutex
	rerr   error

	whandshake bool

	whlock sync.Mutex
	whsync bool
	werr   error

	protos []string
	con    io.ReadWriteCloser
}

func (l *lazyConn) Read(b []byte) (int, error) {
	if !l.rhandshake {
		go l.writeHandshake()
		err := l.readHandshake()
		if err != nil {
			return 0, err
		}

		l.rhandshake = true
	}

	if len(b) == 0 {
		return 0, nil
	}

	return l.con.Read(b)
}

func (l *lazyConn) readHandshake() error {
	l.rhlock.Lock()
	defer l.rhlock.Unlock()

	// if we've already done this, exit
	if l.rhsync {
		return l.rerr
	}
	l.rhsync = true

	for _, proto := range l.protos {
		// read protocol
		tok, err := ReadNextToken(l.con)
		if err != nil {
			l.rerr = err
			return err
		}

		if tok != proto {
			l.rerr = fmt.Errorf("protocol mismatch in lazy handshake ( %s != %s )", tok, proto)
			return l.rerr
		}
	}

	return nil
}

func (l *lazyConn) writeHandshake() error {
	l.whlock.Lock()
	defer l.whlock.Unlock()

	if l.whsync {
		return l.werr
	}

	l.whsync = true

	buf := bufio.NewWriter(l.con)
	for _, proto := range l.protos {
		err := delimWrite(buf, []byte(proto))
		if err != nil {
			l.werr = err
			return err
		}
	}

	l.werr = buf.Flush()
	return l.werr
}

func (l *lazyConn) Write(b []byte) (int, error) {
	if !l.whandshake {
		go l.readHandshake()
		err := l.writeHandshake()
		if err != nil {
			return 0, err
		}

		l.whandshake = true
	}

	return l.con.Write(b)
}

func (l *lazyConn) Close() error {
	return l.con.Close()
}
