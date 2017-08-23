package websocket

import (
	"net"
	"time"

	ws "gx/ipfs/QmZH5VXfAJouGMyCCHTRPGCT3e5MG9Lu78Ln3YAYW1XTts/websocket"
)

var _ net.Conn = (*Conn)(nil)

// Conn implements net.Conn interface for gorilla/websocket.
type Conn struct {
	*ws.Conn
	DefaultMessageType int
	done               func()
}

func (c *Conn) Read(b []byte) (n int, err error) {
	_, r, err := c.Conn.NextReader()
	if err != nil {
		return 0, err
	}

	return r.Read(b)
}

func (c *Conn) Write(b []byte) (n int, err error) {
	if err := c.Conn.WriteMessage(c.DefaultMessageType, b); err != nil {
		return 0, err
	}

	return len(b), nil
}

func (c *Conn) Close() error {
	if c.done != nil {
		c.done()
	}

	return c.Conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.Conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}

	return c.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.Conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.Conn.SetWriteDeadline(t)
}

// NewConn creates a Conn given a regular gorilla/websocket Conn.
func NewConn(raw *ws.Conn, done func()) *Conn {
	return &Conn{
		Conn:               raw,
		DefaultMessageType: ws.BinaryMessage,
		done:               done,
	}
}
