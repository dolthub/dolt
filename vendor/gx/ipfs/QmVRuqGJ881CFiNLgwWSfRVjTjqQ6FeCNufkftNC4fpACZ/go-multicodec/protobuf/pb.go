package mc_pb

import (
	"errors"
	"io"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
)

var Header []byte
var HeaderMsgio []byte

var ErrNotProtobuf = errors.New("not a protobuf")

func init() {
	Header = mc.Header([]byte("/protobuf"))
	HeaderMsgio = mc.Header([]byte("/protobuf/msgio"))
}

type codec struct {
	mc    bool
	msgio bool
}

func Codec(m proto.Message) mc.Codec {
	return &codec{mc: false, msgio: true} // cannot do without atm.
}

func Multicodec(m proto.Message) mc.Multicodec {
	return &codec{mc: true, msgio: true} // cannot do without atm.
}

func (c *codec) Encoder(w io.Writer) mc.Encoder {
	return &encoder{
		w:   w,
		buf: proto.NewBuffer(nil),
		c:   c,
	}
}

func (c *codec) Decoder(r io.Reader) mc.Decoder {
	return &decoder{
		r: r,
		c: c,
	}
}

func (c *codec) Header() []byte {
	if c.msgio {
		return HeaderMsgio
	}
	return Header
}

type encoder struct {
	w   io.Writer
	buf *proto.Buffer
	c   *codec
}

type decoder struct {
	r io.Reader
	c *codec
}

func (c *encoder) Encode(v interface{}) error {
	w := c.w

	if c.c.mc {
		// if multicodec, write the header first
		if _, err := c.w.Write(c.c.Header()); err != nil {
			return err
		}
	}
	if c.c.msgio {
		w = msgio.NewWriter(w)
	}

	pbv, ok := v.(proto.Message)
	if !ok {
		return ErrNotProtobuf
	}

	defer c.buf.Reset()
	if err := c.buf.Marshal(pbv); err != nil {
		return err
	}

	_, err := w.Write(c.buf.Bytes())
	return err
}

func (c *decoder) Decode(v interface{}) error {
	pbv, ok := v.(proto.Message)
	if !ok {
		return ErrNotProtobuf
	}

	if c.c.mc {
		// if multicodec, consume the header first
		if err := mc.ConsumeHeader(c.r, c.c.Header()); err != nil {
			return err
		}
	}

	if c.c.msgio {
		msg, err := msgio.NewReader(c.r).ReadMsg()
		if err != nil {
			return err
		}
		return proto.Unmarshal(msg, pbv)
	}

	return errors.New("protobuf without msgio not supported yet")
}
