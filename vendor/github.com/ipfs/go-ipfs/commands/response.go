package commands

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
)

// ErrorType signfies a category of errors
type ErrorType uint

// ErrorTypes convey what category of error ocurred
const (
	ErrNormal         ErrorType = iota // general errors
	ErrClient                          // error was caused by the client, (e.g. invalid CLI usage)
	ErrImplementation                  // programmer error in the server
	ErrNotFound                        // == HTTP 404 Not Found
	// TODO: add more types of errors for better error-specific handling
)

// Error is a struct for marshalling errors
type Error struct {
	Message string
	Code    ErrorType
}

func (e Error) Error() string {
	return e.Message
}

// EncodingType defines a supported encoding
type EncodingType string

// Supported EncodingType constants.
const (
	JSON     = "json"
	XML      = "xml"
	Protobuf = "protobuf"
	Text     = "text"
	// TODO: support more encoding types
)

func marshalJson(value interface{}) (io.Reader, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	return bytes.NewReader(b), nil
}

var marshallers = map[EncodingType]Marshaler{
	JSON: func(res Response) (io.Reader, error) {
		ch, ok := res.Output().(<-chan interface{})
		if ok {
			return &ChannelMarshaler{
				Channel:   ch,
				Marshaler: marshalJson,
				Res:       res,
			}, nil
		}

		var value interface{}
		if res.Error() != nil {
			value = res.Error()
		} else {
			value = res.Output()
		}
		return marshalJson(value)
	},
	XML: func(res Response) (io.Reader, error) {
		var value interface{}
		if res.Error() != nil {
			value = res.Error()
		} else {
			value = res.Output()
		}

		b, err := xml.Marshal(value)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(b), nil
	},
}

// Response is the result of a command request. Handlers write to the response,
// setting Error or Value. Response is returned to the client.
type Response interface {
	Request() Request

	// Set/Return the response Error
	SetError(err error, code ErrorType)
	Error() *Error

	// Sets/Returns the response value
	SetOutput(interface{})
	Output() interface{}

	// Sets/Returns the length of the output
	SetLength(uint64)
	Length() uint64

	// underlying http connections need to be cleaned up, this is for that
	Close() error
	SetCloser(io.Closer)

	// Marshal marshals out the response into a buffer. It uses the EncodingType
	// on the Request to chose a Marshaler (Codec).
	Marshal() (io.Reader, error)

	// Gets a io.Reader that reads the marshalled output
	Reader() (io.Reader, error)

	// Gets Stdout and Stderr, for writing to console without using SetOutput
	Stdout() io.Writer
	Stderr() io.Writer
}

type response struct {
	req    Request
	err    *Error
	value  interface{}
	out    io.Reader
	length uint64
	stdout io.Writer
	stderr io.Writer
	closer io.Closer
}

func (r *response) Request() Request {
	return r.req
}

func (r *response) Output() interface{} {
	return r.value
}

func (r *response) SetOutput(v interface{}) {
	r.value = v
}

func (r *response) Length() uint64 {
	return r.length
}

func (r *response) SetLength(l uint64) {
	r.length = l
}

func (r *response) Error() *Error {
	return r.err
}

func (r *response) SetError(err error, code ErrorType) {
	r.err = &Error{Message: err.Error(), Code: code}
}

func (r *response) Marshal() (io.Reader, error) {
	if r.err == nil && r.value == nil {
		return bytes.NewReader([]byte{}), nil
	}

	enc, found, err := r.req.Option(EncShort).String()
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("No encoding type was specified")
	}
	encType := EncodingType(strings.ToLower(enc))

	// Special case: if text encoding and an error, just print it out.
	if encType == Text && r.Error() != nil {
		return strings.NewReader(r.Error().Error()), nil
	}

	var marshaller Marshaler
	if r.req.Command() != nil && r.req.Command().Marshalers != nil {
		marshaller = r.req.Command().Marshalers[encType]
	}
	if marshaller == nil {
		var ok bool
		marshaller, ok = marshallers[encType]
		if !ok {
			return nil, fmt.Errorf("No marshaller found for encoding type '%s'", enc)
		}
	}

	output, err := marshaller(r)
	if err != nil {
		return nil, err
	}
	if output == nil {
		return bytes.NewReader([]byte{}), nil
	}
	return output, nil
}

// Reader returns an `io.Reader` representing marshalled output of this Response
// Note that multiple calls to this will return a reference to the same io.Reader
func (r *response) Reader() (io.Reader, error) {
	if r.out == nil {
		if out, ok := r.value.(io.Reader); ok {
			// if command returned a io.Reader, use that as our reader
			r.out = out

		} else {
			// otherwise, use the response marshaler output
			marshalled, err := r.Marshal()
			if err != nil {
				return nil, err
			}

			r.out = marshalled
		}
	}

	return r.out, nil
}

func (r *response) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

func (r *response) SetCloser(c io.Closer) {
	r.closer = c
}

func (r *response) Stdout() io.Writer {
	return r.stdout
}

func (r *response) Stderr() io.Writer {
	return r.stderr
}

// NewResponse returns a response to match given Request
func NewResponse(req Request) Response {
	return &response{
		req:    req,
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}
