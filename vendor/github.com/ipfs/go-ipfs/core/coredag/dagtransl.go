package coredag

import (
	"fmt"
	"io"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

// DagParser is function used for parsing stream into Node
type DagParser func(r io.Reader, mhType uint64, mhLen int) ([]node.Node, error)

// FormatParsers is used for mapping format descriptors to DagParsers
type FormatParsers map[string]DagParser

// InputEncParsers is used for mapping input encodings to FormatParsers
type InputEncParsers map[string]FormatParsers

// DefaultInputEncParsers is InputEncParser that is used everywhere
var DefaultInputEncParsers = InputEncParsers{
	"json": defaultJSONParsers,
	"raw":  defaultRawParsers,
	"cbor": defaultCborParsers,
}

var defaultJSONParsers = FormatParsers{
	"cbor":     cborJSONParser,
	"dag-cbor": cborJSONParser,

	"protobuf": dagpbJSONParser,
	"dag-pb":   dagpbJSONParser,
}

var defaultRawParsers = FormatParsers{
	"cbor":     cborRawParser,
	"dag-cbor": cborRawParser,

	"protobuf": dagpbRawParser,
	"dag-pb":   dagpbRawParser,

	"raw": rawRawParser,
}

var defaultCborParsers = FormatParsers{
	"cbor":     cborRawParser,
	"dag-cbor": cborRawParser,
}

// ParseInputs uses DefaultInputEncParsers to parse io.Reader described by
// input encoding and format to an instance of ipld Node
func ParseInputs(ienc, format string, r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	return DefaultInputEncParsers.ParseInputs(ienc, format, r, mhType, mhLen)
}

// AddParser adds DagParser under give input encoding and format
func (iep InputEncParsers) AddParser(ienv, format string, f DagParser) {
	m, ok := iep[ienv]
	if !ok {
		m = make(FormatParsers)
		iep[ienv] = m
	}

	m[format] = f
}

// ParseInputs parses io.Reader described by input encoding and format to
// an instance of ipld Node
func (iep InputEncParsers) ParseInputs(ienc, format string, r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	parsers, ok := iep[ienc]
	if !ok {
		return nil, fmt.Errorf("no input parser for %q", ienc)
	}

	parser, ok := parsers[format]
	if !ok {
		return nil, fmt.Errorf("no parser for format %q using input type %q", format, ienc)
	}

	return parser(r, mhType, mhLen)
}
