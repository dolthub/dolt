package main

import (
	crand "crypto/rand"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	randomfiles "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-random-files"
	ringreader "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-random-files/ringreader"
)

var usage = `usage: %s [options] <path>...
Write a random filesystem hierarchy to each <path>

Options:
`

// flags
var opts randomfiles.Options
var quiet bool
var alphabet string
var paths []string
var cryptorand bool

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		flag.PrintDefaults()
	}

	flag.BoolVar(&quiet, "q", false, "quiet output")
	flag.BoolVar(&cryptorand, "random-crypto", false, "use cryptographic randomness for files")
	flag.StringVar(&alphabet, "alphabet", "easy", "alphabet for filenames {easy, hard}")
	flag.IntVar(&opts.FileSize, "filesize", 4096, "filesize - how big to make each file (or max)")

	flag.IntVar(&opts.FanoutDepth, "depth", 2, "fanout depth - how deep the hierarchy goes")
	flag.IntVar(&opts.FanoutDirs, "dirs", 5, "fanout dirs - number of dirs per dir (or max)")
	flag.IntVar(&opts.FanoutFiles, "files", 10, "fanout files - number of files per dir (or max")

	flag.Int64Var(&opts.RandomSeed, "seed", 0, "random seed - 0 for current time")
	flag.BoolVar(&opts.RandomFanout, "random-fanout", false, "randomize fanout numbers")
	flag.BoolVar(&opts.RandomSize, "random-size", true, "randomize filesize")
}

func parseArgs() error {
	flag.Parse()

	switch alphabet {
	case "easy":
		opts.Alphabet = randomfiles.RunesEasy
	case "hard":
		opts.Alphabet = randomfiles.RunesHard
	default:
		return errors.New("alphabet must be one of: easy, hard")
	}

	paths = flag.Args()
	if len(paths) < 1 {
		flag.Usage()
		os.Exit(0)
	}

	if !quiet {
		opts.Out = os.Stdout
	}

	switch opts.RandomSeed {
	case 0:
		rand.Seed(time.Now().UnixNano())
	default:
		rand.Seed(opts.RandomSeed)
	}

	// prepare randomn source.
	if cryptorand {
		opts.Source = crand.Reader
	} else {
		// if not crypto, we don't need a lot of random
		// data. we just need to sample from a sequence.
		s := 16777216 // 16MB
		r, err := ringreader.NewReader(s)
		if err != nil {
			return err
		}
		opts.Source = r
	}

	return nil
}

func run() error {
	if err := parseArgs(); err != nil {
		return err
	}

	for _, root := range paths {
		if err := os.MkdirAll(root, 0755); err != nil {
			return err
		}

		err := randomfiles.WriteRandomFiles(root, 1, &opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
