package bin

import (
	"encoding/hex"
	"io"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	base "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/base"
)

var (
	HeaderPath = "/base16/"
	Header     = mc.Header([]byte(HeaderPath))
	multic     = mc.NewMulticodecFromCodec(Codec(), Header)
)

type codec struct{}

type decoder struct {
	r io.Reader
}

func (d decoder) Decode(v interface{}) error {
	out, ok := v.([]byte)
	if !ok {
		return base.ErrExpectedByteSlice
	}

	buf := make([]byte, hex.EncodedLen(len(out)))
	_, err := d.r.Read(buf)
	if err != nil {
		return err
	}
	_, err = hex.Decode(out, buf)
	return err
}

func (codec) Decoder(r io.Reader) mc.Decoder {
	return decoder{r}
}

type encoder struct {
	w io.Writer
}

func (e encoder) Encode(v interface{}) error {
	slice, ok := v.([]byte)
	if !ok {
		return base.ErrExpectedByteSlice
	}

	buf := make([]byte, hex.EncodedLen(len(slice)))
	hex.Encode(buf, slice)

	_, err := e.w.Write(buf)
	return err
}

func (codec) Encoder(w io.Writer) mc.Encoder {
	return encoder{w}
}

func Codec() mc.Codec {
	return codec{}
}

func Multicodec() mc.Multicodec {
	return multic
}
