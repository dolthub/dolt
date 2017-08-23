package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	cbor "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/cbor"
	json "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/json"
	mux "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/mux"
)

// flags
var Flags FlagType

type FlagType struct {
	Command string
	Args    []string

	Msgio   bool
	MCWrap  bool
	Headers bool
	Paths   bool
}

func (f *FlagType) Arg(i int) string {
	n := i + 1
	if len(f.Args) < n {
		die(fmt.Sprintf("expected %d argument(s)", n))
	}
	return f.Args[i]
}

var usage = `
multicodec - tool to inspect and manipulate mixed codec streams

Usage
    cat rawjson | multicodec wrap /json/msgio >mcjson
    cat rawcbor | multicodec wrap /cbor >mccbor

    cat mixed | multicodec recode /json/msgio >all_in_json
    cat mixed | multicodec filter /json/msgio >json_ones_only

    cat mixed | multicodec headers >all_headers
    cat mixed | multicodec paths >all_paths

    cat paths   | multicodec p2h >headers
    cat headers | multicodec h2p >paths

    multicodec header /json >json_header
    multicodec header /protobuf/msgio >pb_header

Commands
    filter <path>   filter items of given codec
    recode <path>   recode items to given codec
    wrap   <path>   wrap raw data with header
    header <path>   make a header of given size

    headers         output only items' headers
    paths           output only items' header paths
    h2p             convert headers to line-delimited paths
    p2h             convert line-delimited paths to headers

Options
    --mcwrap        item headers wrapped with /multicodec
    --msgio         wrap all subcodecs with /msgio
`

func init() {
	flag.BoolVar(&Flags.MCWrap, "mcwrap", false, "items headers wrapped with /multicodec")
	flag.BoolVar(&Flags.Msgio, "msgio", false, "wrap all subcodecs with /msgio")
	flag.BoolVar(&Flags.Headers, "headers", false, "output only the headers")
	flag.BoolVar(&Flags.Paths, "paths", false, "output only the header paths")
	flag.Usage = func() {
		fmt.Println(strings.TrimSpace(usage))
		os.Exit(0)
	}
}

func die(err string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
	os.Exit(-1)
}

func main() {
	if err := run(); err != nil {
		die(err.Error())
	}
}

func argParse() {
	flag.Parse()

	if l := len(flag.Args()); l < 1 || l > 2 {
		flag.Usage()
	}

	Flags.Command = flag.Args()[0]
	Flags.Args = flag.Args()[1:]
}

func run() error {
	argParse()

	w := os.Stdout
	r := os.Stdin

	switch Flags.Command {
	case "header":
		return header(w, Flags.Arg(0))
	case "headers":
		return headers(w, r)
	case "paths":
		return paths(w, r)
	case "wrap":
		return wrap(w, r, Flags.Arg(0))
	case "filter":
		return filter(w, r, Flags.Arg(0))
	case "recode":
		return recode(w, r, Flags.Arg(0))
	case "h2p":
		return h2p(w, r)
	case "p2h":
		return p2h(w, r)
	default:
		flag.Usage()
		return nil
	}
}

func h2p(w io.Writer, r io.Reader) error {
	for {
		hdr, err := mc.ReadHeader(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		p := string(mc.HeaderPath(hdr))

		_, err = fmt.Fprintln(w, p)
		if err != nil {
			return err
		}
	}
}

func p2h(w io.Writer, r io.Reader) error {
	br := bufio.NewReader(r)

	for {
		p, err := br.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		p = p[:len(p)-1] // remove \n

		hdr := mc.Header(p)

		_, err = w.Write(hdr)
		if err != nil {
			return err
		}
	}
}

func header(w io.Writer, path string) error {
	_, err := w.Write(mc.Header([]byte(path)))
	return err
}

func headers(w io.Writer, r io.Reader) error {
	return decode(r, func(codec *mux.Multicodec, v interface{}) error {
		_, err := w.Write(codec.Last.Header())
		return err
	})
}

func paths(w io.Writer, r io.Reader) error {
	return decode(r, func(codec *mux.Multicodec, v interface{}) error {
		p := mc.HeaderPath(codec.Last.Header())
		_, err := fmt.Fprintln(w, string(p))
		return err
	})
}

func wrap(w io.Writer, r io.Reader, path string) error {
	mcc := codecWithPath(path)
	if mcc == nil {
		return fmt.Errorf("unknown codec %s", path)
	}
	hdrs := string(mcc.Header())

	wrapRT := func(cc mc.Codec, mcc mc.Multicodec) error {
		var v interface{}
		if err := cc.Decoder(r).Decode(&v); err != nil {
			return err
		}
		return mcc.Encoder(w).Encode(&v)
	}

	switch hdrs {
	case string(json.HeaderMsgio):
		return wrapRT(json.Codec(true), mcc)
	case string(json.Header):
		return wrapRT(json.Codec(false), mcc)
	case string(cbor.Header):
		return wrapRT(cbor.Codec(), mcc)
	}

	return fmt.Errorf("wrap unsupported for codec %s", hdrs)
}

func filter(w io.Writer, r io.Reader, path string) error {
	hdr := mc.Header([]byte(path))

	// as we decode, write everything to a buffer.
	buf := bytes.NewBuffer(nil)
	r = io.TeeReader(r, buf)

	return decode(r, func(codec *mux.Multicodec, value interface{}) error {
		defer buf.Reset()

		// c.Last is the last multicodec encoded or decoded.
		// skip headers which do not match.
		if !bytes.Equal(hdr, codec.Last.Header()) {
			return nil
		}

		_, err := io.Copy(w, buf)
		if err != nil && err != io.EOF {
			return err
		}
		return nil
	})
}

func recode(w io.Writer, r io.Reader, path string) error {
	codec := codecWithPath(path)
	if codec == nil {
		return fmt.Errorf("unknown codec %s", path)
	}
	enc := codec.Encoder(w)

	return decode(r, func(codec *mux.Multicodec, v interface{}) error {
		return enc.Encode(v)
	})
}

func decode(r io.Reader, next func(m *mux.Multicodec, v interface{}) error) error {
	c := codec()
	dec := c.Decoder(r)

	for {
		var v interface{}
		if err := dec.Decode(&v); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := next(c, v); err != nil {
			return err
		}
	}

	return nil
}

func codecWithPath(path string) mc.Multicodec {
	hdr := mc.Header([]byte(path))
	codec := mux.CodecWithHeader(hdr, mux.StandardMux().Codecs)
	return codec
}

func codec() *mux.Multicodec {
	m := mux.StandardMux()
	m.Wrap = Flags.MCWrap
	return m
}
