// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// This script runs random noms commands against random datasets on a database.
//
// Example usage:
// > go run path/to/loadtest.go http://demo.noms.io/cli-tour
//
// Imports should be Go builtin libraries only, so that this can be run with "go run".

type runnerFn func(db, ds string)

type runner struct {
	name string
	fn   runnerFn
}

func main() {
	rand.Seed(time.Now().UnixNano() + bestEffortGetIP())

	if len(os.Args) != 2 {
		fmt.Println("Usage: loadtest <database>")
		os.Exit(-1)
	}

	db := os.Args[1]

	rs := []runner{
		{"diff", runDiff},
		{"log diff", runLogDiff},
		{"log show", runLogShow},
		{"show", runShow},
		{"sync", runSync},
	}

	for ds := range streamDs(db) {
		start := time.Now()
		r := rs[rand.Intn(len(rs))]
		fmt.Println(time.Now().Format(time.Stamp), r.name, db, ds)
		r.fn(db, fmt.Sprintf("%s::%s", db, ds))
		fmt.Println("  took", time.Since(start).String())
	}
}

func bestEffortGetIP() (asNum int64) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				asNum = int64(binary.BigEndian.Uint32([]byte(ipnet.IP.To4())))
				break
			}
		}
	}
	return
}

func runDiff(db, ds string) {
	if parent := getParent(db, ds); parent != "" {
		call(nil, "noms", "diff", ds, parent)
	} else {
		fmt.Println("    (no parent, cannot diff)")
	}
}

func runLogDiff(db, ds string) {
	call(nil, "noms", "log", ds)
}

func runLogShow(db, ds string) {
	call(nil, "noms", "log", "--show-value", ds)
}

func runShow(db, ds string) {
	if strings.HasSuffix(ds, "/raw") {
		fmt.Println("    (skipping raw file, blobs are too slow)")
	} else {
		call(nil, "noms", "show", ds)
	}
}

func runSync(db, ds string) {
	dir, err := ioutil.TempDir("", "loadtest")
	if err != nil {
		fmt.Fprintln(os.Stderr, "  ERROR: failed to create temp directory:", err.Error())
		return
	}

	defer os.RemoveAll(dir)
	// Try to sync to parent, then from parent to head.
	// If there isn't a parent then just sync head.
	syncDs := fmt.Sprintf("ldb:%s::sync", dir)
	if parent := getParent(db, ds); parent != "" {
		call(nil, "noms", "sync", parent, syncDs)
	}
	call(nil, "noms", "sync", ds, syncDs)
}

func getParent(db, ds string) string {
	buf := &bytes.Buffer{}
	call(buf, "noms", "log", "-n", "2", "--oneline", ds)
	// Output will look like:
	// abc (Parent def)
	// def (Parent None)
	// We could use the first line and grab the Parent value from there, but it could also be Merge,
	// and it might be None, so easier to just get the 2nd row.
	lines := strings.SplitN(buf.String(), "\n", 2)
	if len(lines) != 2 {
		return ""
	}
	hsh := strings.SplitN(lines[0], " ", 2)[0]
	return fmt.Sprintf("%s::#%s", db, hsh)
}

func call(stdout io.Writer, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	fmt.Println("    >", name, strings.Join(arg, " "))
	cmd.Stdout = stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "    ERROR: %s\n", err.Error())
	}
	return err
}

func streamDs(db string) <-chan string {
	buf := &bytes.Buffer{}
	err := call(buf, "noms", "ds", db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "    ERROR: failed to get datasets")
		os.Exit(-1)
	}

	out := strings.Trim(buf.String(), " \n")
	if out == "" {
		fmt.Fprintln(os.Stderr, "    ERROR: no datasets at", db)
		os.Exit(-1)
	}

	datasets := strings.Split(out, "\n")

	ch := make(chan string)
	go func() {
		for {
			ch <- datasets[rand.Intn(len(datasets))]
		}
	}()
	return ch
}
