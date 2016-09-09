// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	human "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

var (
	start time.Time
)

func main() {
	noProgress := flag.Bool("no-progress", false, "prevents progress from being output if true")
	performCommit := flag.Bool("commit", true, "commit the data to head of the dataset (otherwise only write the data to the dataset)")
	stdin := flag.Bool("stdin", false, "read blob from stdin")

	spec.RegisterCommitMetaFlags(flag.CommandLine)
	spec.RegisterDatabaseFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Fetches a URL, file, or stdin into a noms blob\n\nUsage: %s [--stdin?] [url-or-local-path?] [dataset]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse(true)

	if !(*stdin && flag.NArg() == 1) && flag.NArg() != 2 {
		flag.Usage()
		os.Exit(-1)
	}

	start = time.Now()

	ds, err := spec.GetDataset(flag.Arg(flag.NArg() - 1))
	d.CheckErrorNoUsage(err)
	defer ds.Database().Close()

	var r io.Reader
	var contentLength int64
	var sourceType, sourceVal string

	if *stdin {
		r = os.Stdin
		contentLength = -1
	} else if url := flag.Arg(0); strings.HasPrefix(url, "http") {
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

		r = resp.Body
		contentLength = resp.ContentLength
		sourceType, sourceVal = "url", url
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

		r = f
		contentLength = s.Size()
		sourceType, sourceVal = "file", url
	}

	if !*noProgress {
		r = progressreader.New(r, getStatusPrinter(contentLength))
	}
	b := types.NewStreamingBlob(r, ds.Database())

	if *performCommit {
		var additionalMetaInfo map[string]string
		if sourceType != "" {
			additionalMetaInfo = map[string]string{sourceType: sourceVal}
		}
		meta, err := spec.CreateCommitMetaStruct(ds.Database(), "", "", additionalMetaInfo, nil)
		d.CheckErrorNoUsage(err)
		ds, err = ds.Commit(b, dataset.CommitOptions{Meta: meta})
		if err != nil {
			d.Chk.Equal(datas.ErrMergeNeeded, err)
			fmt.Fprintf(os.Stderr, "Could not commit, optimistic concurrency failed.")
			return
		}
		if !*noProgress {
			status.Done()
		}
	} else {
		ref := ds.Database().WriteValue(b)
		if !*noProgress {
			status.Clear()
		}
		fmt.Fprintf(os.Stdout, "#%s\n", ref.TargetHash().String())
	}
}

func getStatusPrinter(expectedLen int64) progressreader.Callback {
	return func(seenLen uint64) {
		var expected string
		if expectedLen < 0 {
			expected = "(unknown)"
		} else {
			expected = human.Bytes(uint64(expectedLen))
		}

		elapsed := time.Now().Sub(start)
		rate := uint64(float64(seenLen) / elapsed.Seconds())

		status.Printf("%s of %s written in %ds (%s/s)...",
			human.Bytes(seenLen),
			expected,
			uint64(elapsed.Seconds()),
			human.Bytes(rate))
	}
}
