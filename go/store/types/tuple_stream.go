// Copyright 2021 Dolthub, Inc.
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

package types

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

// TupleWriter is an interface for an object that supports Types.Tuples being written to it
type TupleWriter interface {
	// WriteTuples writes the provided tuples
	WriteTuples(...Tuple) error
	// WriteNull write a null to the stream
	WriteNull() error
	// CopyFrom reads tuples from a reader and writes them
	CopyFrom(TupleReader) error
}

// TupleReader is an interface for an object that supports reading types.Tuples
type TupleReader interface {
	// Read reades the next tuple from the TupleReader
	Read() (*Tuple, error)
}

// Closer is an interface for a class that can be closed
type Closer interface {
	// Close should release any underlying resources
	Close(context.Context) error
}

// TupleWriteCloser is an interface for a TupleWriter that has a Close method
type TupleWriteCloser interface {
	TupleWriter
	Closer
}

// TupleReadCloser is an interface for a TupleReader that has a Close method
type TupleReadCloser interface {
	TupleReader
	Closer
}

type tupleWriterImpl struct {
	wr io.Writer
}

// NewTupleWriter returns a TupleWriteCloser that writes tuple data to the supplied io.Writer
func NewTupleWriter(wr io.Writer) TupleWriteCloser {
	return &tupleWriterImpl{wr: wr}
}

var nullBytes [4]byte

func init() {
	binary.BigEndian.PutUint32(nullBytes[:], 0)
}

func (twr *tupleWriterImpl) WriteNull() error {
	return iohelp.WriteAll(twr.wr, nullBytes[:])
}

func (twr *tupleWriterImpl) write(t Tuple) error {
	size := len(t.buff)

	var sizeBytes [4]byte
	binary.BigEndian.PutUint32(sizeBytes[:], uint32(size))
	err := iohelp.WriteAll(twr.wr, sizeBytes[:])
	if err != nil {
		return err
	}

	return iohelp.WriteAll(twr.wr, t.buff)
}

// WriteTuples writes the provided tuples
func (twr *tupleWriterImpl) WriteTuples(tuples ...Tuple) error {
	for _, t := range tuples {
		err := twr.write(t)

		if err != nil {
			return err
		}
	}

	return nil
}

// CopyFrom reads tuples from a reader and writes them
func (twr *tupleWriterImpl) CopyFrom(rd TupleReader) error {
	for {
		t, err := rd.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if t != nil {
			err = twr.write(*t)
		} else {
			err = twr.WriteNull()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Close should release any underlying resources
func (twr *tupleWriterImpl) Close(context.Context) error {
	closer, ok := twr.wr.(io.Closer)
	if ok {
		return closer.Close()
	}

	return nil
}

type tupleReaderImpl struct {
	nbf *NomsBinFormat
	vrw ValueReadWriter
	rd  io.Reader
}

// NewTupleReader returns a TupleReadCloser that reads tuple data from the supplied io.Reader
func NewTupleReader(nbf *NomsBinFormat, vrw ValueReadWriter, rd io.Reader) TupleReadCloser {
	return &tupleReaderImpl{nbf: nbf, vrw: vrw, rd: rd}
}

// Read reades the next tuple from the TupleReader
func (trd *tupleReaderImpl) Read() (*Tuple, error) {
	sizeBytes := make([]byte, 4)
	_, err := io.ReadFull(trd.rd, sizeBytes)
	if err != nil {
		return nil, err
	}

	// Nulls are encoded as 0 sized
	size := binary.BigEndian.Uint32(sizeBytes)
	if size == 0 {
		return nil, nil
	}

	data := make([]byte, size)
	_, err = io.ReadFull(trd.rd, data)
	if err != nil {
		if err == io.EOF {
			return nil, errors.New("corrupt tuple stream")
		}
		return nil, err
	}

	return &Tuple{valueImpl{trd.vrw, trd.nbf, data, nil}}, nil
}

// Close should release any underlying resources
func (trd *tupleReaderImpl) Close(context.Context) error {
	closer, ok := trd.rd.(io.Closer)
	if ok {
		return closer.Close()
	}

	return nil
}

type TupleReadingEditProvider struct {
	rd         TupleReader
	reachedEOF bool
}

func TupleReaderAsEditProvider(rd TupleReader) EditProvider {
	return &TupleReadingEditProvider{rd: rd}
}

func (t TupleReadingEditProvider) Next(ctx context.Context) (*KVP, error) {
	k, err := t.rd.Read()

	if err == io.EOF {
		t.reachedEOF = true
		return nil, io.EOF
	} else if err != nil {
		return nil, err
	}

	v, err := t.rd.Read()

	if err == io.EOF {
		return nil, errors.New("corrupt tuple stream")
	} else if err != nil {
		return nil, err
	}

	if v == nil {
		return &KVP{Key: k}, nil
	}

	return &KVP{Key: k, Val: *v}, nil
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (t TupleReadingEditProvider) ReachedEOF() bool {
	return t.reachedEOF
}

func (t TupleReadingEditProvider) Close(ctx context.Context) error {
	if closer, ok := t.rd.(Closer); ok {
		return closer.Close(ctx)
	}

	return nil
}
