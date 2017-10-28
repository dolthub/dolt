package muxcodec

import (
	"bytes"
	"fmt"
	"io"

	mc "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec"
)

var (
	ErrNoCodec = fmt.Errorf("no suitable codec")
)

var Header []byte

func init() {
	Header = mc.Header([]byte("/multicodec"))
}

// SelectCodec is a function that selects which codecs are able to
// marshal a given datastructure, and orders them (to be tried first).
type SelectCodec func(v interface{}, codecs []mc.Multicodec) mc.Multicodec

// SelectFirst is the default SelectCodec function. selects the first
// codec given.
func SelectFirst(v interface{}, codecs []mc.Multicodec) mc.Multicodec {
	return codecs[0]
}

// MuxMulticodec returns a multicodec that muxes between given codecs.
// It uses the given SelectCodec function when marshaling, to select
// the best codec to use.
func MuxMulticodec(codecs []mc.Multicodec, sel SelectCodec) *Multicodec {
	if sel == nil {
		sel = SelectFirst
	}
	return &Multicodec{codecs, sel, true, nil}
}

type Multicodec struct {
	Codecs []mc.Multicodec // subcodecs to use
	Select SelectCodec     // pick a subcodec for encoding
	Wrap   bool            // whether to wrap with own header
	Last   mc.Multicodec   // the last subcodec used
}

func (c *Multicodec) Encoder(w io.Writer) mc.Encoder {
	return &encoder{w, c}
}

func (c *Multicodec) Decoder(r io.Reader) mc.Decoder {
	return &decoder{r, c}
}

func (c *Multicodec) Header() []byte {
	return Header
}

type encoder struct {
	w io.Writer
	c *Multicodec
}

type decoder struct {
	r io.Reader
	c *Multicodec
}

func (c *encoder) Encode(v interface{}) error {
	subc := c.c.Select(v, c.c.Codecs)
	if subc == nil {
		return ErrNoCodec
	}

	if c.c.Wrap { // write multicodec header.
		if _, err := c.w.Write(c.c.Header()); err != nil {
			return err
		}
	}

	c.c.Last = subc
	return subc.Encoder(c.w).Encode(v)
}

func (c *decoder) Decode(v interface{}) error {
	if c.c.Wrap { // read multicodec header.
		if err := mc.ConsumeHeader(c.r, c.c.Header()); err != nil {
			return err
		}
	}

	// get next header, to select codec
	hdr, err := mc.ReadHeader(c.r)
	if err != nil {
		return err
	}

	// "unwind" the read as subc consumes header
	r := mc.WrapHeaderReader(hdr, c.r)

	subc := CodecWithHeader(hdr, c.c.Codecs)
	if subc == nil {
		return fmt.Errorf("no codec for %s", hdr)
	}

	c.c.Last = subc
	return subc.Decoder(r).Decode(v)
}

func CodecWithHeader(hdr []byte, codecs []mc.Multicodec) mc.Multicodec {
	// we'll look through the list. should be small.
	// if huge, consider a map.
	for _, c := range codecs {
		if bytes.Equal(hdr, c.Header()) {
			return c
		}
	}
	return nil
}
