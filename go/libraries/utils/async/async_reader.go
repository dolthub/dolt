// Copyright 2020 Liquidata, Inc.
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

package async

import (
	"context"
	"io"
	"sync"
)

// ReadFunc is a function that is called repeatedly in order to retrieve a stream of objects.  When all objects have been
// been read from the stream then (nil, io.EOF) should be returned.
type ReadFunc func(ctx context.Context) (interface{}, error)

type objErrTuple struct {
	obj interface{}
	err error
}

// AsyncReader is a TableReadCloser implementation that spins up a go routine to keep reading data into
// a buffer so that it is ready when the caller wants it.
type AsyncReader struct {
	readFunc ReadFunc
	stopCh   chan struct{}
	rowCh    chan objErrTuple
	wg       *sync.WaitGroup
}

// NewAsyncReader creates a new AsyncReader
func NewAsyncReader(rf ReadFunc, bufferSize int) *AsyncReader {
	return &AsyncReader{rf, make(chan struct{}), make(chan objErrTuple, bufferSize), &sync.WaitGroup{}}
}

// Start the worker routine reading rows to the channel
func (asRd *AsyncReader) Start(ctx context.Context) error {
	asRd.wg.Add(1)
	go func() {
		defer asRd.wg.Done()
		defer close(asRd.rowCh)
		asRd.readObjects(ctx)
	}()

	return nil
}

// ReadObject reads an object
func (asRd *AsyncReader) Read() (interface{}, error) {
	objErrTup := <-asRd.rowCh

	if objErrTup.obj == nil && objErrTup.err == nil {
		return nil, io.EOF
	}

	return objErrTup.obj, objErrTup.err
}

// Close releases resources being held
func (asRd *AsyncReader) Close() error {
	close(asRd.stopCh)
	asRd.wg.Wait()

	return nil
}

// background read loop running in separate go routine
func (asRd *AsyncReader) readObjects(ctx context.Context) {
	for {
		select {
		case <-asRd.stopCh:
			return
		default:
		}

		obj, err := asRd.readFunc(ctx)
		asRd.rowCh <- objErrTuple{obj, err}

		if err != nil {
			break
		}
	}
}
