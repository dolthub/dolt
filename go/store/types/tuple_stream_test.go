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
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var _ TupleReader = (*TestTupleStream)(nil)
var _ TupleWriter = (*TestTupleStream)(nil)

type TestTupleStream struct {
	mu     *sync.Mutex
	tuples []*Tuple
	i      int
	closed bool
}

func NewTestTupleStream(initialSize int) *TestTupleStream {
	var tuples []Tuple
	if initialSize > 0 {
		tuples = make([]Tuple, 0, initialSize)
	}

	return NewTestTupleStreamFromTuples(tuples)
}

func NewTestTupleStreamFromTuples(tuples []Tuple) *TestTupleStream {
	tuplePtrs := make([]*Tuple, len(tuples))
	for i := range tuples {
		tuplePtrs[i] = &tuples[i]
	}
	return &TestTupleStream{
		mu:     &sync.Mutex{},
		tuples: tuplePtrs,
	}
}

func (st *TestTupleStream) WriteNull() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return errClosed
	}

	st.tuples = append(st.tuples, nil)
	return nil
}

func (st *TestTupleStream) WriteTuples(t ...Tuple) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return errClosed
	}

	for i := 0; i < len(t); i++ {
		st.tuples = append(st.tuples, &t[i])
	}
	return nil
}

func (st *TestTupleStream) CopyFrom(rd TupleReader) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return errClosed
	}

	for {
		t, err := rd.Read()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		st.tuples = append(st.tuples, t)
	}
}

func (st *TestTupleStream) Read() (*Tuple, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return nil, errClosed
	}

	if st.i >= len(st.tuples) {
		return nil, io.EOF
	}

	t := st.tuples[st.i]
	st.i++

	return t, nil
}

func (st *TestTupleStream) Close(ctx context.Context) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return errClosed
	}

	st.closed = true
	return nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func genTuples(t *testing.T, num int) []Tuple {
	const numCols = 5

	tuples := make([]Tuple, num)
	fields := make([]Value, numCols)

	for i := 0; i < num; i++ {
		uid, err := uuid.NewUUID()
		require.NoError(t, err)

		fields[0] = Uint(0)
		fields[1] = Int(i)
		fields[2] = Float(rand.Float64())
		fields[3] = String(randString(128))
		fields[4] = UUID(uid)

		tuples[i], err = NewTuple(Format_Default, fields...)
		require.NoError(t, err)
	}

	return tuples
}

func TestTupleReadersAndWriters(t *testing.T) {
	const numTuples = 100_000
	tuples := genTuples(t, numTuples)

	t.Run("test read/writers", func(t *testing.T) {
		src := NewTestTupleStreamFromTuples(tuples)
		dest := NewTestTupleStream(0)
		err := dest.CopyFrom(src)
		require.NoError(t, err)

		err = dest.WriteTuples(tuples[0])
		require.NoError(t, err)
		err = dest.WriteTuples(tuples[1:]...)
		require.NoError(t, err)

		read := make([]*Tuple, 2*numTuples)
		for i := 0; i < 2; i++ {
			for j := 0; j < numTuples; j++ {
				read[i*numTuples+j], err = dest.Read()
				require.NoError(t, err)
			}
		}

		_, err = dest.Read()
		require.Equal(t, io.EOF, err)

		for i := 0; i < numTuples; i++ {
			require.True(t, tuples[i].Equals(*read[i]))
			require.True(t, tuples[i].Equals(*read[numTuples+i]))
		}
	})

	t.Run("mem read/writers", func(t *testing.T) {
		src := NewTestTupleStreamFromTuples(tuples)

		buf := bytes.NewBuffer(nil)
		wr := NewTupleWriter(buf)
		err := wr.CopyFrom(src)
		require.NoError(t, err)

		err = wr.WriteTuples(tuples[0])
		require.NoError(t, err)
		err = wr.WriteTuples(tuples[1:]...)
		require.NoError(t, err)

		vrw := NewMemoryValueStore()
		rd := NewTupleReader(Format_Default, vrw, io.NopCloser(buf))

		read := make([]*Tuple, 2*numTuples)
		for i := 0; i < 2; i++ {
			for j := 0; j < numTuples; j++ {
				read[i*numTuples+j], err = rd.Read()
				require.NoError(t, err)
			}
		}

		_, err = rd.Read()
		require.Equal(t, io.EOF, err)

		for i := 0; i < numTuples; i++ {
			require.True(t, tuples[i].Equals(*read[i]))
			require.True(t, tuples[i].Equals(*read[numTuples+i]))
		}
	})
}
