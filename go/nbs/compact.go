// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"io"

	"github.com/attic-labs/noms/go/d"
)

var maxInt = int64(math.MaxInt64)

func init() {
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
	}
}

func compact(dir string, mt *memTable, haver chunkReader) (name addr, chunkCount uint32) {
	tempName, h, chunkCount := func() (string, addr, uint32) {
		temp, err := ioutil.TempFile(dir, "nbs_table_")
		d.PanicIfError(err)
		defer checkClose(temp)

		maxSize := maxTableSize(uint64(len(mt.order)), mt.totalData)
		buff := make([]byte, maxSize)
		tw := newTableWriter(buff)
		count := mt.write(tw, haver)
		tableSize, h := tw.finish()
		io.Copy(temp, bytes.NewReader(buff[:tableSize]))

		return temp.Name(), h, count
	}()
	if chunkCount > 0 {
		err := os.Rename(tempName, filepath.Join(dir, h.String()))
		d.PanicIfError(err)
	} else {
		os.Remove(tempName)
	}
	return h, chunkCount
}
