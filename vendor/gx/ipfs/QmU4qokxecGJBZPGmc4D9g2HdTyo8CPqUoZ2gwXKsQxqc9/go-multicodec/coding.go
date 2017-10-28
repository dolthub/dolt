package multicodec

import (
	"bytes"
	"io"
)

// Codec is an algorithm for coding data from one representation
// to another. For convenience, we define a codec in the usual
// sense: a function and its inverse, to encode and decode.
type Codec interface {
	// Decoder wraps given io.Reader and returns an object which
	// will decode bytes into objects.
	Decoder(r io.Reader) Decoder

	// Encoder wraps given io.Writer and returns an Encoder
	Encoder(w io.Writer) Encoder
}

// Encoder encodes objects into bytes and writes them to an
// underlying io.Writer. Works like encoding.Marshal
type Encoder interface {
	Encode(n interface{}) error
}

// Decoder decodes objects from bytes from an underlying
// io.Reader, into given object. Works like encoding.Unmarshal
type Decoder interface {
	Decode(n interface{}) error
}

// Marshal serializes an object to a []byte.
func Marshal(c Codec, o interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := MarshalTo(c, &buf, o)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalTo serializes an object to a writer.
func MarshalTo(c Codec, w io.Writer, o interface{}) error {
	return c.Encoder(w).Encode(o)
}

// Unmarshal deserializes an object to a []byte.
func Unmarshal(c Codec, buf []byte, o interface{}) error {
	return UnmarshalFrom(c, bytes.NewBuffer(buf), o)
}

// UnmarshalFrom deserializes an objects from a reader.
func UnmarshalFrom(c Codec, r io.Reader, o interface{}) error {
	return c.Decoder(r).Decode(o)
}
