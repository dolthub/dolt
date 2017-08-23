package mc_json

import (
	"bytes"
	"io"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
)

var Header []byte

func init() {
	Header = mc.Header([]byte("/msgio"))
}

type codec struct {
	mc bool
}

func Codec() mc.Codec {
	return &codec{mc: false}
}

func Multicodec() mc.Multicodec {
	return &codec{mc: true}
}

func (c *codec) Encoder(w io.Writer) mc.Encoder {
	return &encoder{
		w:  w,
		mc: c.mc,
	}
}

func (c *codec) Decoder(r io.Reader) mc.Decoder {
	return &decoder{
		r:  r,
		mc: c.mc,
	}
}

func (c *codec) Header() []byte {
	return Header
}

type encoder struct {
	w  io.Writer
	mc bool
}

type decoder struct {
	r  io.Reader
	mc bool
}

func (c *encoder) Encode(v interface{}) error {
	var r io.Reader
	switch v := v.(type) {
	case []byte:
		r = bytes.NewReader(v)
	case io.Reader:
		r = v
	default:
		return mc.ErrType
	}

	if c.mc {
		// if multicodec, write the header first
		if _, err := c.w.Write(Header); err != nil {
			return err
		}
	}

	w := msgio.NewLimitedWriter(c.w)
	if _, err := io.Copy(w, r); err != nil {
		return err
	}

	return w.Flush()
}

func (c *decoder) Decode(v interface{}) error {
	var w io.Writer
	switch v := v.(type) {
	case []byte:
		w = &bufWriter{B: v}
	case io.Writer:
		w = v
	default:
		return mc.ErrType
	}

	if c.mc {
		// if multicodec, consume the header first
		if err := mc.ConsumeHeader(c.r, Header); err != nil {
			return err
		}
	}

	r, err := msgio.LimitedReader(c.r)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, r)
	return err
}

type bufWriter struct {
	L int
	B []byte
}

func (w *bufWriter) Write(buf []byte) (n int, err error) {
	b := w.B[w.L:]
	if len(buf) > len(b) {
		buf = buf[:len(b)]
	}

	n = copy(b, buf)
	if n < len(buf) {
		err = io.ErrShortBuffer
	}
	w.L += n

	return n, err
}
