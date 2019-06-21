// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/go/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	"github.com/liquidata-inc/ld/dolt/go/store/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/go/nbs/benchmarks/gen"
)

var readFile = flag.String("input-file", "", "A file full of test data. Creates and saves associated .chunks file at runtime if it doesn't yet exist. If none is specified, data and .chunks files will be generated and saved.")

type hashSlice []hash.Hash

func (s hashSlice) Len() int {
	return len(s)
}

func (s hashSlice) Less(i, j int) bool {
	return s[i].Less(s[j])
}

func (s hashSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type dataSource struct {
	data, cm            *os.File
	totalData, dataRead uint64
	hashes              hashSlice
}

func getInput(dataSize uint64) (src *dataSource, err error) {
	filename := *readFile
	if filename == "" {
		filename = humanize.IBytes(dataSize) + ".bin"
	}
	data, err := gen.OpenOrGenerateDataFile(filename, dataSize)
	if err != nil {
		return nil, err
	}
	chunkFile := data.Name() + ".chunks"
	cm := gen.OpenOrBuildChunkMap(chunkFile, data)
	fmt.Println("Reading from", filename, "with chunks", chunkFile)
	src = &dataSource{data: data, cm: cm, totalData: dataSize}
	tuples := make(chan offsetTuple, 1024)
	go func() {
		src.readTuples(tuples)
		close(tuples)
	}()
	for ot := range tuples {
		src.hashes = append(src.hashes, ot.h)
	}
	return src, err
}

type offsetTuple struct {
	h hash.Hash
	l uint64
}

func (src *dataSource) PrimeFilesystemCache() {
	bufData := bufio.NewReaderSize(src.data, 10*humanize.MiByte)
	tuples := make(chan offsetTuple, 16)
	go func() {
		src.readTuples(tuples)
		close(tuples)
	}()

	for ot := range tuples {
		buff := make([]byte, ot.l)
		n, err := io.ReadFull(bufData, buff)
		d.Chk.NoError(err)
		d.Chk.True(uint64(n) == ot.l)
	}
}

func (src *dataSource) ReadChunks(chunkChan chan<- *chunks.Chunk) {
	bufData := bufio.NewReaderSize(src.data, humanize.MiByte)
	tuples := make(chan offsetTuple, 1024)
	go func() {
		src.readTuples(tuples)
		close(tuples)
	}()

	for ot := range tuples {
		buff := make([]byte, ot.l)
		n, err := io.ReadFull(bufData, buff)
		d.Chk.NoError(err)
		d.Chk.True(uint64(n) == ot.l)
		c := chunks.NewChunkWithHash(ot.h, buff)
		chunkChan <- &c
	}
}

func (src *dataSource) GetHashes() hashSlice {
	out := make(hashSlice, len(src.hashes))
	copy(out, src.hashes)
	return out
}

func (src *dataSource) readTuples(tuples chan<- offsetTuple) {
	src.reset()

	otBuf := [gen.OffsetTupleLen]byte{}
	cm := bufio.NewReaderSize(src.cm, humanize.MiByte)
	ot := offsetTuple{}

	for src.dataRead < src.totalData {
		n, err := io.ReadFull(cm, otBuf[:])
		if err != nil {
			d.Chk.True(err == io.EOF)
			return
		}
		d.Chk.True(n == gen.OffsetTupleLen)
		ot.h = hash.New(otBuf[:20])
		ot.l = uint64(binary.BigEndian.Uint32(otBuf[20:]))
		src.dataRead += ot.l
		tuples <- ot
	}
}

func (src *dataSource) reset() {
	_, err := src.data.Seek(0, io.SeekStart)
	d.Chk.NoError(err)
	_, err = src.cm.Seek(0, io.SeekStart)
	d.Chk.NoError(err)
	src.dataRead = 0
}

func (src *dataSource) Close() {
	d.Chk.NoError(src.data.Close())
	d.Chk.NoError(src.cm.Close())
}
