package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/attic-labs/buzhash"
	humanize "github.com/dustin/go-humanize"
)

func main() {
	useSHA1 := flag.Bool("use-sha1", false, "whether we sha1 the bytes")
	useBH := flag.Bool("use-bh", false, "whether we buzhash the bytes")
	flag.Parse()

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
	h := sha1.New()
	bh := buzhash.NewBuzHash(64 * 8)
	f, _ := os.Open(p)
	defer f.Close()
	t0 := time.Now()
	buf := make([]byte, 4*1024)
	l := uint64(0)

	for {
		n, err := f.Read(buf)
		l += uint64(n)
		if err == io.EOF {
			break
		}
		s := buf[:n]
		if *useSHA1 {
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
	fmt.Printf("%x\n", h.Sum(digest))
}
