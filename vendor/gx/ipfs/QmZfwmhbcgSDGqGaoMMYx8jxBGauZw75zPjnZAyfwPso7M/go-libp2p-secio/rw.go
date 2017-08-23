package secio

import (
	"context"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
	mpool "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio/mpool"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

// ErrMACInvalid signals that a MAC verification failed
var ErrMACInvalid = errors.New("MAC verification failed")

// bufPool is a ByteSlicePool for messages. we need buffers because (sadly)
// we cannot encrypt in place-- the user needs their buffer back.
var bufPool = mpool.ByteSlicePool

type etmWriter struct {
	// params
	pool mpool.Pool    // for the buffers with encrypted data
	str  cipher.Stream // the stream cipher to encrypt with
	mac  HMAC          // the mac to authenticate data with
	w    io.Writer

	sync.Mutex
}

// NewETMWriter Encrypt-Then-MAC
func NewETMWriter(w io.Writer, s cipher.Stream, mac HMAC) msgio.WriteCloser {
	return &etmWriter{w: w, str: s, mac: mac, pool: bufPool}
}

// Write writes passed in buffer as a single message.
func (w *etmWriter) Write(b []byte) (int, error) {
	if err := w.WriteMsg(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteMsg writes the msg in the passed in buffer.
func (w *etmWriter) WriteMsg(b []byte) error {
	w.Lock()
	defer w.Unlock()

	bufsize := uint32(4 + len(b) + w.mac.Size())
	// encrypt.
	buf := w.pool.Get(bufsize).([]byte)
	data := buf[4 : 4+len(b)] // the pool's buffer may be larger
	w.str.XORKeyStream(data, b)

	// log.Debugf("ENC plaintext (%d): %s %v", len(b), b, b)
	// log.Debugf("ENC ciphertext (%d): %s %v", len(data), data, data)

	// then, mac.
	if _, err := w.mac.Write(data); err != nil {
		return err
	}

	// Sum appends.
	data = w.mac.Sum(data)
	w.mac.Reset()
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))

	_, err := w.w.Write(buf[:bufsize])
	w.pool.Put(bufsize, buf)
	return err
}

func (w *etmWriter) Close() error {
	if c, ok := w.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type etmReader struct {
	msgio.Reader
	io.Closer

	// internal buffer returned from the msgio
	buf []byte

	// low and high watermark for the buffered data
	lowat int
	hiwat int

	// params
	msg msgio.ReadCloser // msgio for knowing where boundaries lie
	str cipher.Stream    // the stream cipher to encrypt with
	mac HMAC             // the mac to authenticate data with

	sync.Mutex
}

// NewETMReader Encrypt-Then-MAC
func NewETMReader(r io.Reader, s cipher.Stream, mac HMAC) msgio.ReadCloser {
	return &etmReader{msg: msgio.NewReader(r), str: s, mac: mac}
}

func (r *etmReader) NextMsgLen() (int, error) {
	return r.msg.NextMsgLen()
}

func (r *etmReader) drain(buf []byte) int {
	// Return zero if there is no data remaining in the internal buffer.
	if r.lowat == r.hiwat {
		return 0
	}

	// Copy data to the output buffer.
	n := copy(buf, r.buf[r.lowat:r.hiwat])

	// Update the low watermark.
	r.lowat += n

	// Release the buffer and reset the watermarks if it has been fully read.
	if r.lowat == r.hiwat {
		r.msg.ReleaseMsg(r.buf)
		r.buf = nil
		r.lowat = 0
		r.hiwat = 0
	}

	return n
}

func (r *etmReader) fill() error {
	// Read a message from the underlying msgio.
	msg, err := r.msg.ReadMsg()
	if err != nil {
		return err
	}

	// Check the MAC.
	n, err := r.macCheckThenDecrypt(msg)
	if err != nil {
		r.msg.ReleaseMsg(msg)
		return err
	}

	// Retain the buffer so it can be drained from and later released.
	r.buf = msg
	r.lowat = 0
	r.hiwat = n

	return nil
}

func (r *etmReader) Read(buf []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	// Return buffered data without reading more, if possible.
	copied := r.drain(buf)
	if copied > 0 {
		return copied, nil
	}

	// Check the length of the next message.
	fullLen, err := r.msg.NextMsgLen()
	if err != nil {
		return 0, err
	}

	// If the destination buffer is too short, fill an internal buffer and then
	// drain as much of that into the output buffer as will fit.
	if cap(buf) < fullLen {
		err := r.fill()
		if err != nil {
			return 0, err
		}

		copied := r.drain(buf)
		return copied, nil
	}

	// Otherwise, read directly into the destination buffer.
	n, err := io.ReadFull(r.msg, buf[:fullLen])
	if err != nil {
		return 0, err
	}

	m, err := r.macCheckThenDecrypt(buf[:n])
	if err != nil {
		return 0, err
	}

	return m, nil
}

func (r *etmReader) ReadMsg() ([]byte, error) {
	r.Lock()
	defer r.Unlock()

	msg, err := r.msg.ReadMsg()
	if err != nil {
		return nil, err
	}

	n, err := r.macCheckThenDecrypt(msg)
	if err != nil {
		return nil, err
	}
	return msg[:n], nil
}

func (r *etmReader) macCheckThenDecrypt(m []byte) (int, error) {
	l := len(m)
	if l < r.mac.size {
		return 0, fmt.Errorf("buffer (%d) shorter than MAC size (%d)", l, r.mac.size)
	}

	mark := l - r.mac.size
	data := m[:mark]
	macd := m[mark:]

	r.mac.Write(data)
	expected := r.mac.Sum(nil)
	r.mac.Reset()

	// check mac. if failed, return error.
	if !hmac.Equal(macd, expected) {
		log.Debug("MAC Invalid:", expected, "!=", macd)
		return 0, ErrMACInvalid
	}

	// ok seems good. decrypt. (can decrypt in place, yay!)
	// log.Debugf("DEC ciphertext (%d): %s %v", len(data), data, data)
	r.str.XORKeyStream(data, data)
	// log.Debugf("DEC plaintext (%d): %s %v", len(data), data, data)

	return mark, nil
}

func (r *etmReader) Close() error {
	return r.msg.Close()
}

// ReleaseMsg signals a buffer can be reused.
func (r *etmReader) ReleaseMsg(b []byte) {
	r.msg.ReleaseMsg(b)
}

// writeMsgCtx is used by the
func writeMsgCtx(ctx context.Context, w msgio.Writer, msg proto.Message) ([]byte, error) {
	enc, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// write in a goroutine so we can exit when our context is cancelled.
	done := make(chan error)
	go func(m []byte) {
		err := w.WriteMsg(m)
		select {
		case done <- err:
		case <-ctx.Done():
		}
	}(enc)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-done:
		return enc, e
	}
}

func readMsgCtx(ctx context.Context, r msgio.Reader, p proto.Message) ([]byte, error) {
	var msg []byte

	// read in a goroutine so we can exit when our context is cancelled.
	done := make(chan error)
	go func() {
		var err error
		msg, err = r.ReadMsg()
		select {
		case done <- err:
		case <-ctx.Done():
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-done:
		if e != nil {
			return nil, e
		}
	}

	return msg, proto.Unmarshal(msg, p)
}
