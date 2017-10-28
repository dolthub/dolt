package multicodec

import "io"

// Multicodec is the interface for a multicodec
type Multicodec interface {
	Codec

	Header() []byte
}

type c2mc struct {
	c      Codec
	header []byte
}

var _ Multicodec = (*c2mc)(nil)

func (c c2mc) Header() []byte {
	return c.header
}

type c2mcD struct {
	base *c2mc
	r    io.Reader
}

func (d c2mcD) Decode(v interface{}) error {
	err := ConsumeHeader(d.r, d.base.header)
	if err != nil {
		return err
	}
	return d.base.c.Decoder(d.r).Decode(v)
}

var _ Decoder = (*c2mcD)(nil)

func (c c2mc) Decoder(r io.Reader) Decoder {
	return c2mcD{
		base: &c,
		r:    r,
	}
}

type c2mcE struct {
	base *c2mc
	w    io.Writer
}

func (e c2mcE) Encode(v interface{}) error {
	_, err := e.w.Write(e.base.Header())
	if err != nil {
		return err
	}
	return e.base.c.Encoder(e.w).Encode(v)
}

func (c c2mc) Encoder(w io.Writer) Encoder {
	return c2mcE{
		base: &c,
		w:    w,
	}
}

func NewMulticodecFromCodec(c Codec, header []byte) Multicodec {
	return c2mc{
		c:      c,
		header: header,
	}
}
