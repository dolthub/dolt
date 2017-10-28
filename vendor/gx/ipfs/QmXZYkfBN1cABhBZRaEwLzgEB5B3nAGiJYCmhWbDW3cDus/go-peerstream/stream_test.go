package peerstream

import (
	"errors"
	"io"
	"sync"

	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

type fakeSmuxStream struct {
	smux.Stream
	conn      *fakeSmuxConn
	closeLock sync.Mutex
	closed    chan struct{}
}

func (fss *fakeSmuxStream) Close() error {
	fss.closeLock.Lock()
	defer fss.closeLock.Unlock()

	select {
	case <-fss.conn.closed:
		return errors.New("already closed")
	case <-fss.closed:
		return errors.New("already closed")
	default:
	}
	close(fss.closed)
	return nil
}

func (fss *fakeSmuxStream) Read(b []byte) (int, error) {
	select {
	case <-fss.closed:
	case <-fss.conn.closed:
	}
	return 0, io.EOF
}

func (fss *fakeSmuxStream) Write(b []byte) (int, error) {
	select {
	case <-fss.closed:
	case <-fss.conn.closed:
	}
	return 0, errors.New("connection closed")
}

func (fss *fakeSmuxStream) Reset() error {
	return fss.Close()
}
