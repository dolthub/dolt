// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func BulkStat(parallelism int, files []string) {
	todo := make(chan string, len(files))
	var wg sync.WaitGroup
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			for {
				fn := <-todo
				if fn == "" {
					break
				}
				_, err := os.Lstat(fn)
				if err != nil {
					log.Fatal("All stats should succeed:", err)
				}
			}
			wg.Done()
		}()
	}

	for _, v := range files {
		todo <- v
	}
	close(todo)
	wg.Wait()
}

func ReadLines(name string) []string {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal("ReadLines: ", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	l := []string{}
	for {
		line, _, err := r.ReadLine()
		if line == nil || err != nil {
			break
		}

		fn := string(line)
		l = append(l, fn)
	}
	if len(l) == 0 {
		log.Fatal("no files added")
	}

	return l
}

func main() {
	N := flag.Int("N", 1000, "how many files to stat")
	cpu := flag.Int("cpu", 1, "how many threads to use")
	prefix := flag.String("prefix", "", "mount point")
	quiet := flag.Bool("quiet", false, "be quiet")
	flag.Parse()

	f := flag.Arg(0)
	files := ReadLines(f)
	for i, f := range files {
		files[i] = filepath.Join(*prefix, f)
	}
	if !*quiet {
		log.Printf("statting %d with %d threads; first file %s (%d names)", *N, *cpu, files[0], len(files))
	}
	todo := *N
	for todo > 0 {
		if len(files) > todo {
			files = files[:todo]
		}
		BulkStat(*cpu, files)
		todo -= len(files)
	}
}
