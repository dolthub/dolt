package websocket

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

type wsConn struct {
	manet.Conn
	t tpt.Transport
}

var _ tpt.Conn = (*wsConn)(nil)

func (c *wsConn) Transport() tpt.Transport {
	return c.t
}

type listener struct {
	manet.Listener

	incoming chan *Conn

	tpt tpt.Transport

	origin *url.URL
}

func (l *listener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed to upgrade websocket", 400)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	l.incoming <- NewConn(c, cancel)

	// wait until conn gets closed, otherwise the handler closes it early
	<-ctx.Done()
}

func (l *listener) Accept() (tpt.Conn, error) {
	c, ok := <-l.incoming
	if !ok {
		return nil, fmt.Errorf("listener is closed")
	}

	mnc, err := manet.WrapNetConn(c)
	if err != nil {
		return nil, err
	}

	return &wsConn{
		Conn: mnc,
		t:    l.tpt,
	}, nil
}

func (l *listener) Multiaddr() ma.Multiaddr {
	wsma, err := ma.NewMultiaddr("/ws")
	if err != nil {
		panic(err)
	}

	return l.Listener.Multiaddr().Encapsulate(wsma)
}
