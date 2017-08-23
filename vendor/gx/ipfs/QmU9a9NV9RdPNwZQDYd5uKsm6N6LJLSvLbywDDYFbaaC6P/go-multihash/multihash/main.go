package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	mhopts "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash/opts"
)

var usage = `usage: %s [options] [FILE]
Print or check multihash checksums.
With no FILE, or when FILE is -, read standard input.

Options:
`

// flags
var opts *mhopts.Options
var checkRaw string
var checkMh mh.Multihash
var inputFilename string
var quiet bool
var help bool

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		flag.PrintDefaults()
	}

	opts = mhopts.SetupFlags(flag.CommandLine)

	checkStr := "check checksum matches"
	flag.StringVar(&checkRaw, "check", "", checkStr)
	flag.StringVar(&checkRaw, "c", "", checkStr+" (shorthand)")

	helpStr := "display help message"
	flag.BoolVar(&help, "help", false, helpStr)
	flag.BoolVar(&help, "h", false, helpStr+" (shorthand)")

	quietStr := "quiet output (no newline on checksum, no error text)"
	flag.BoolVar(&quiet, "quiet", false, quietStr)
	flag.BoolVar(&quiet, "q", false, quietStr+" (shorthand)")
}

func parseFlags(o *mhopts.Options) error {
	flag.Parse()
	if err := o.ParseError(); err != nil {
		return err
	}

	if checkRaw != "" {
		var err error
		checkMh, err = mhopts.Decode(o.Encoding, checkRaw)
		if err != nil {
			return fmt.Errorf("fail to decode check '%s': %s", checkRaw, err)
		}
	}

	return nil
}

func getInput() (io.ReadCloser, error) {
	args := flag.Args()

	switch {
	case len(args) < 1:
		inputFilename = "-"
		return os.Stdin, nil
	case args[0] == "-":
		inputFilename = "-"
		return os.Stdin, nil
	default:
		inputFilename = args[0]
		f, err := os.Open(args[0])
		if err != nil {
			return nil, fmt.Errorf("failed to open '%s': %s", args[0], err)
		}
		return f, nil
	}
}
func printHash(o *mhopts.Options, r io.Reader) error {
	h, err := o.Multihash(r)
	if err != nil {
		return err
	}

	s, err := mhopts.Encode(o.Encoding, h)
	if err != nil {
		return err
	}

	if quiet {
		fmt.Print(s)
	} else {
		fmt.Println(s)
	}
	return nil
}

func main() {
	checkErr := func(err error) {
		if err != nil {
			die("error: ", err)
		}
	}

	err := parseFlags(opts)
	checkErr(err)

	if help {
		flag.Usage()
		os.Exit(0)
	}

	inp, err := getInput()
	checkErr(err)

	if checkMh != nil {
		err = opts.Check(inp, checkMh)
		checkErr(err)
		if !quiet {
			fmt.Println("OK checksums match (-q for no output)")
		}
	} else {
		err = printHash(opts, inp)
		checkErr(err)
	}
	inp.Close()
}

func die(v ...interface{}) {
	if !quiet {
		fmt.Fprint(os.Stderr, v...)
		fmt.Fprint(os.Stderr, "\n")
	}
	// flag.Usage()
	os.Exit(1)
}
