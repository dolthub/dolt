package bin

import (
	"io"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	base "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/base"
)

var (
	HeaderPath = "/bin/"
	Header     = mc.Header([]byte(HeaderPath))
	multic     = mc.NewMulticodecFromCodec(Codec(), Header)
)

type codec struct{}

func (codec) Header() []byte {
	return Header
}

type decoder struct {
	r io.Reader
}

func (d decoder) Decode(v interface{}) error {
	slice, ok := v.([]byte)
	if !ok {
		return base.ErrExpectedByteSlice
	}

	_, err := d.r.Read(slice)
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

	_, err := e.w.Write(slice)
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
