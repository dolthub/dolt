package mc_json

import (
	"bytes"
	"encoding/json"
	"io"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"

	mc "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec"
)

var HeaderPath string
var Header []byte
var HeaderMsgioPath string
var HeaderMsgio []byte

func init() {
	HeaderPath = "/json"
	HeaderMsgioPath = "/json/msgio"
	Header = mc.Header([]byte(HeaderPath))
	HeaderMsgio = mc.Header([]byte(HeaderMsgioPath))
}

type codec struct {
	mc    bool
	msgio bool
}

func Codec(msgio bool) mc.Codec {
	return &codec{mc: false, msgio: msgio}
}

func Multicodec(msgio bool) mc.Multicodec {
	return &codec{mc: true, msgio: msgio}
}

func (c *codec) Encoder(w io.Writer) mc.Encoder {
	buf := bytes.NewBuffer(nil)
	return &encoder{
		w:   w,
		c:   c,
		buf: buf,
		enc: json.NewEncoder(buf),
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
	c   *codec
	enc *json.Encoder
	buf *bytes.Buffer
}

type decoder struct {
	r io.Reader
	c *codec
}

func (c *encoder) Encode(v interface{}) error {
	defer c.buf.Reset()
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

	// recast to deal with map[interface{}]interface{} case
	vr, err := recast(v)
	if err != nil {
		return err
	}

	if err := c.enc.Encode(vr); err != nil {
		return err
	}

	_, err = io.Copy(w, c.buf)
	return err
}

func (c *decoder) Decode(v interface{}) error {
	r := c.r

	if c.c.mc {
		// if multicodec, consume the header first
		if err := mc.ConsumeHeader(c.r, c.c.Header()); err != nil {
			return err
		}
	}
	if c.c.msgio {
		// need to make a new one per read.
		var err error
		r, err = msgio.LimitedReader(c.r)
		if err != nil {
			return err
		}
	}

	return json.NewDecoder(r).Decode(v)
}

func recast(v interface{}) (cv interface{}, err error) {
	switch v.(type) {
	case map[interface{}]interface{}:
		vmi := v.(map[interface{}]interface{})
		vms := make(map[string]interface{}, len(vmi))
		for k, v2 := range vmi {
			ks, ok := k.(string)
			if !ok {
				return v, mc.ErrType
			}

			rv2, err := recast(v2)
			if err != nil {
				return v, err
			}

			vms[ks] = rv2
		}
		return vms, nil
	default:
		return v, nil // hope for the best.
	}
}
