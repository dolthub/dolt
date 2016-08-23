// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/codahale/blake2"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
	"github.com/kch42/buzhash"
)

func main() {
	useSHA := flag.String("use-sha", "", "<default>=no hashing, 1=sha1, 256=sha256, 512=sha512, blake=blake2b")
	useBH := flag.Bool("use-bh", false, "whether we buzhash the bytes")
	flag.Parse(true)

	flag.Usage = func() {
		fmt.Printf("%s <big-file>\n", os.Args[0])
		flag.PrintDefaults()
		return
	}

	if len(flag.Args()) < 1 {
		flag.Usage()
		return
	}

	p := flag.Args()[0]
	bh := buzhash.NewBuzHash(64 * 8)
	f, _ := os.Open(p)
	defer f.Close()
	t0 := time.Now()
	buf := make([]byte, 4*1024)
	l := uint64(0)

	var h hash.Hash
	if *useSHA == "1" {
		h = sha1.New()
	} else if *useSHA == "256" {
		h = sha256.New()
	} else if *useSHA == "512" {
		h = sha512.New()
	} else if *useSHA == "blake" {
		h = blake2.NewBlake2B()
	}

	for {
		n, err := f.Read(buf)
		l += uint64(n)
		if err == io.EOF {
			break
		}
		s := buf[:n]
		if h != nil {
			h.Write(s)
		}
		if *useBH {
			bh.Write(s)
		}
	}

	t1 := time.Now()
	d := t1.Sub(t0)
	fmt.Printf("Read  %s in %s (%s/s)\n", humanize.Bytes(l), d, humanize.Bytes(uint64(float64(l)/d.Seconds())))
	digest := []byte{}
	if h != nil {
		fmt.Printf("%x\n", h.Sum(digest))
	}
}
