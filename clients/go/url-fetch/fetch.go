package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	human "github.com/dustin/go-humanize"
)

const (
	clearLine = "\x1b[2K\r"
)

var (
	start time.Time
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Fetches a URL into a noms blob\n\nUsage: %s <dataset> <url>:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flags.RegisterDataStoreFlags()
	flag.Parse()

	if flag.NArg() != 2 {
		util.CheckError(errors.New("expected dataset and url arguments"))
	}

	spec, err := flags.ParseDatasetSpec(flag.Arg(0))
	if err != nil {
		util.CheckError(err)
	}
	ds, err := spec.Dataset()
	util.CheckError(err)

	url := flag.Arg(1)
	start = time.Now()

	var sr statusReader

	if strings.HasPrefix(url, "http") {
		resp, err := http.Get(url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not fetch url %s, error: %s\n", url, err)
			return
		}

		switch resp.StatusCode / 100 {
		case 4, 5:
			fmt.Fprintf(os.Stderr, "Could not fetch url %s, error: %d (%s)\n", url, resp.StatusCode, resp.Status)
			return
		}

		sr = statusReader{
			r:           resp.Body,
			expectedLen: resp.ContentLength,
		}
	} else {
		// assume it's a file
		f, err := os.Open(url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid URL %s - does not start with 'http' and isn't local file either. fopen error: %s", url, err)
			return
		}

		s, err := f.Stat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not stat file %s: %s", url, err)
			return
		}

		sr = statusReader{
			r:           f,
			expectedLen: s.Size(),
		}
	}

	b := types.NewBlob(&sr)
	ds, err = ds.Commit(b)
	if err != nil {
		d.Chk.Equal(datas.ErrMergeNeeded, err)
		fmt.Fprintf(os.Stderr, "Could not commit, optimistic concurrency failed.")
		return
	}

	fmt.Print(clearLine)
	fmt.Println("Done")
}

type statusReader struct {
	r           io.Reader
	expectedLen int64
	totalRead   uint64
}

func (sr *statusReader) Read(p []byte) (n int, err error) {
	// Print progress before calling Read() since we want to report progress on how far we've
	// written so far, and we don't write until after the Read() call returns.
	var expected string
	if sr.expectedLen < 0 {
		expected = "(unknown)"
	} else {
		expected = human.Bytes(uint64(sr.expectedLen))
	}

	elapsed := time.Now().Sub(start)
	rate := uint64(float64(sr.totalRead) / elapsed.Seconds())

	fmt.Fprintf(os.Stderr, "%s%s of %s written in %ds (%s/s)...",
		clearLine,
		human.Bytes(sr.totalRead),
		expected,
		uint64(elapsed.Seconds()),
		human.Bytes(rate))

	n, err = sr.r.Read(p)
	sr.totalRead += uint64(n)
	return
}
