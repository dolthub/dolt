package b64

import (
	"encoding/base64"
	"io"

	mc "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec"
	base "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec/base"
)

var (
	HeaderPath = "/base64/"
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

	_, err := d.r.Read(out)

	return err
}

func (codec) Decoder(r io.Reader) mc.Decoder {
	return decoder{base64.NewDecoder(base64.StdEncoding, r)}
}

type encoder struct {
	w io.WriteCloser
}

func (e encoder) Encode(v interface{}) error {
	slice, ok := v.([]byte)
	if !ok {
		return base.ErrExpectedByteSlice
	}
	_, err := e.w.Write(slice)
	if err != nil {
		return err
	}
	return e.w.Close()
}

func (codec) Encoder(w io.Writer) mc.Encoder {
	return encoder{base64.NewEncoder(base64.StdEncoding, w)}
}

func Codec() mc.Codec {
	return codec{}
}

func Multicodec() mc.Multicodec {
	return multic
}
