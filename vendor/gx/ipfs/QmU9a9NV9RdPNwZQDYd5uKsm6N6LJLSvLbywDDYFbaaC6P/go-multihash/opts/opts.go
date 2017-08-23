// Package opts helps to write commands which may take multihash
// options.
package opts

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

// package errors
var (
	ErrMatch = errors.New("multihash checksums did not match")
)

// Options is a struct used to parse cli flags.
type Options struct {
	Encoding      string
	Algorithm     string
	AlgorithmCode uint64
	Length        int

	fs *flag.FlagSet
}

// FlagValues are the values the various option flags can take.
var FlagValues = struct {
	Encodings  []string
	Algorithms []string
}{
	Encodings:  []string{"raw", "hex", "base58", "base64"},
	Algorithms: []string{"sha1", "sha2-256", "sha2-512", "sha3"},
}

// SetupFlags adds multihash related options to given flagset.
func SetupFlags(f *flag.FlagSet) *Options {
	// TODO: add arg for adding opt prefix and/or overriding opts

	o := new(Options)
	algoStr := "one of: " + strings.Join(FlagValues.Algorithms, ", ")
	f.StringVar(&o.Algorithm, "algorithm", "sha2-256", algoStr)
	f.StringVar(&o.Algorithm, "a", "sha2-256", algoStr+" (shorthand)")

	encStr := "one of: " + strings.Join(FlagValues.Encodings, ", ")
	f.StringVar(&o.Encoding, "encoding", "base58", encStr)
	f.StringVar(&o.Encoding, "e", "base58", encStr+" (shorthand)")

	lengthStr := "checksums length in bits (truncate). -1 is default"
	f.IntVar(&o.Length, "length", -1, lengthStr)
	f.IntVar(&o.Length, "l", -1, lengthStr+" (shorthand)")
	return o
}

// Parse parses the values of flags from given argument slice.
// It is equivalent to flags.Parse(args)
func (o *Options) Parse(args []string) error {
	if err := o.fs.Parse(args); err != nil {
		return err
	}
	return o.ParseError()
}

// ParseError checks the parsed options for errors.
func (o *Options) ParseError() error {
	if !strIn(o.Encoding, FlagValues.Encodings) {
		return fmt.Errorf("encoding '%s' not %s", o.Encoding, FlagValues.Encodings)
	}

	if !strIn(o.Algorithm, FlagValues.Algorithms) {
		return fmt.Errorf("algorithm '%s' not %s", o.Algorithm, FlagValues.Algorithms)
	}

	var found bool
	o.AlgorithmCode, found = mh.Names[o.Algorithm]
	if !found {
		return fmt.Errorf("algorithm '%s' not found (lib error, pls report).", o.Algorithm)
	}

	if o.Length >= 0 {
		if o.Length%8 != 0 {
			return fmt.Errorf("length must be multiple of 8")
		}
		o.Length = o.Length / 8

		if o.Length > mh.DefaultLengths[o.AlgorithmCode] {
			o.Length = mh.DefaultLengths[o.AlgorithmCode]
		}
	}
	return nil
}

// strIn checks wither string a is in set.
func strIn(a string, set []string) bool {
	for _, s := range set {
		if s == a {
			return true
		}
	}
	return false
}

// Check reads all the data in r, calculates its multihash,
// and checks it matches h1
func (o *Options) Check(r io.Reader, h1 mh.Multihash) error {
	h2, err := o.Multihash(r)
	if err != nil {
		return err
	}

	if !bytes.Equal(h1, h2) {
		return fmt.Errorf("computed checksum did not match")
	}

	return nil
}

// Multihash reads all the data in r and calculates its multihash.
func (o *Options) Multihash(r io.Reader) (mh.Multihash, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return mh.Sum(b, o.AlgorithmCode, o.Length)
}
