package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
)

var Args ArgType

type ArgType struct {
	Command string
	Args    []string
}

func (a *ArgType) Arg(i int) string {
	n := i + 1
	if len(a.Args) < n {
		die(fmt.Sprintf("expected %d argument(s)", n))
	}
	return a.Args[i]
}

var usageStr = `
msgio - tool to wrap messages with msgio header

Usage
    msgio header 1020 >header
    cat file | msgio wrap >wrapped

Commands
    header <size>   output a msgio header of given size
    wrap            wrap incoming stream with msgio
`

func usage() {
	fmt.Println(strings.TrimSpace(usageStr))
	os.Exit(0)
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
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if l := len(args); l < 1 || l > 2 {
		usage()
	}

	Args.Command = flag.Args()[0]
	Args.Args = flag.Args()[1:]
}

func run() error {
	argParse()

	w := os.Stdout
	r := os.Stdin

	switch Args.Command {
	case "header":
		size, err := strconv.Atoi(Args.Arg(0))
		if err != nil {
			return err
		}
		return header(w, size)
	case "wrap":
		return wrap(w, r)
	default:
		usage()
		return nil
	}
}

func header(w io.Writer, size int) error {
	return msgio.WriteLen(w, size)
}

func wrap(w io.Writer, r io.Reader) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	if err := msgio.WriteLen(w, len(buf)); err != nil {
		return err
	}

	_, err = w.Write(buf)
	return err
}
