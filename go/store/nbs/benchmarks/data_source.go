// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"io"
	"os"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nbs/benchmarks/gen"
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

func (src *dataSource) PrimeFilesystemCache() error {
	bufData := bufio.NewReaderSize(src.data, 10*humanize.MiByte)
	tuples := make(chan offsetTuple, 16)

	ae := atomicerr.New()
	go func() {
		err := src.readTuples(tuples)
		ae.SetIfError(err)
		close(tuples)
	}()

	for ot := range tuples {
		if ae.IsSet() {
			break
		}

		buff := make([]byte, ot.l)
		n, err := io.ReadFull(bufData, buff)

		if err != nil {
			return err
		}

		if uint64(n) != ot.l {
			return errors.New("failed to read all data")
		}
	}

	return ae.Get()
}

func (src *dataSource) ReadChunks(chunkChan chan<- *chunks.Chunk) error {
	bufData := bufio.NewReaderSize(src.data, humanize.MiByte)
	tuples := make(chan offsetTuple, 1024)

	ae := atomicerr.New()
	go func() {
		err := src.readTuples(tuples)
		ae.SetIfError(err)
		close(tuples)
	}()

	for ot := range tuples {
		if ae.IsSet() {
			break
		}

		buff := make([]byte, ot.l)
		n, err := io.ReadFull(bufData, buff)

		if err != nil {
			return err
		}

		if uint64(n) != ot.l {
			return errors.New("failed to read the entire chunk")
		}

		c := chunks.NewChunkWithHash(ot.h, buff)
		chunkChan <- &c
	}

	return ae.Get()
}

func (src *dataSource) GetHashes() hashSlice {
	out := make(hashSlice, len(src.hashes))
	copy(out, src.hashes)
	return out
}

func (src *dataSource) readTuples(tuples chan<- offsetTuple) error {
	err := src.reset()

	if err != nil {
		return err
	}

	otBuf := [gen.OffsetTupleLen]byte{}
	cm := bufio.NewReaderSize(src.cm, humanize.MiByte)
	ot := offsetTuple{}

	for src.dataRead < src.totalData {
		n, err := io.ReadFull(cm, otBuf[:])
		if err != nil {
			if err != io.EOF {
				return err
			}

			return nil
		}

		if n != gen.OffsetTupleLen {
			return errors.New("failed to read all data")
		}

		ot.h = hash.New(otBuf[:20])
		ot.l = uint64(binary.BigEndian.Uint32(otBuf[20:]))
		src.dataRead += ot.l
		tuples <- ot
	}

	return nil
}

func (src *dataSource) reset() error {
	_, err := src.data.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	_, err = src.cm.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	src.dataRead = 0

	return nil
}

func (src *dataSource) Close() error {
	dataErr := src.data.Close()
	cmErr := src.cm.Close()

	if dataErr != nil {
		return dataErr
	}

	if cmErr != nil {
		return cmErr
	}

	return nil
}
