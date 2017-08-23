package coredag

import (
	"fmt"
	"io"
	"io/ioutil"

	ipldcbor "gx/ipfs/QmXgUVPAxjMLZSyxx818YstJJAoRg3nyPWENmBLVzLtoax/go-ipld-cbor"
	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

// DagParser is function used for parsing stream into Node
type DagParser func(r io.Reader) ([]node.Node, error)

// FormatParsers is used for mapping format descriptors to DagParsers
type FormatParsers map[string]DagParser

// InputEncParsers is used for mapping input encodings to FormatParsers
type InputEncParsers map[string]FormatParsers

// DefaultInputEncParsers is InputEncParser that is used everywhere
var DefaultInputEncParsers = InputEncParsers{
	"json": defaultJSONParsers,
	"raw":  defaultRawParsers,
}

var defaultJSONParsers = FormatParsers{
	"cbor":     cborJSONParser,
	"dag-cbor": cborJSONParser,
}

var defaultRawParsers = FormatParsers{
	"cbor":     cborRawParser,
	"dag-cbor": cborRawParser,
}

// ParseInputs uses DefaultInputEncParsers to parse io.Reader described by
// input encoding and format to an instance of ipld Node
func ParseInputs(ienc, format string, r io.Reader) ([]node.Node, error) {
	return DefaultInputEncParsers.ParseInputs(ienc, format, r)
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
func (iep InputEncParsers) ParseInputs(ienc, format string, r io.Reader) ([]node.Node, error) {
	pset, ok := iep[ienc]
	if !ok {
		return nil, fmt.Errorf("no input parser for %q", ienc)
	}

	parser, ok := pset[format]
	if !ok {
		return nil, fmt.Errorf("no parser for format %q using input type %q", format, ienc)
	}

	return parser(r)
}

func cborJSONParser(r io.Reader) ([]node.Node, error) {
	nd, err := ipldcbor.FromJson(r)
	if err != nil {
		return nil, err
	}

	return []node.Node{nd}, nil
}

func cborRawParser(r io.Reader) ([]node.Node, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	nd, err := ipldcbor.Decode(data)
	if err != nil {
		return nil, err
	}

	return []node.Node{nd}, nil
}
