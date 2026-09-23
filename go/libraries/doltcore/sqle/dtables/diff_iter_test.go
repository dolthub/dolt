// Copyright 2026 Dolthub, Inc.
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

package dtables

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// A limited query must cancel the asynchronous producer even when its row
// buffer is full. Closing the wrapper used by DOLT_DIFF must reach that producer.
func TestDiffPartitionCloseCancelsProducer(t *testing.T) {
	ctx := sql.NewEmptyContext()
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	rows := make(chan sql.Row, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case rows <- sql.Row{1}:
			case <-child.Done():
				return
			}
		}
	}()
	var source sql.RowIter = prollyDiffIter{rows: rows, errChan: make(chan error), cancel: cancel}
	iter := &diffPartitionRowIter{currentPartition: &DiffPartition{}, currentRowIter: &source}
	for i := 0; i < 51; i++ {
		_, err := iter.Next(ctx)
		require.NoError(t, err)
	}
	require.NoError(t, iter.Close(ctx))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("closing a limited diff did not stop the producer")
	}
	require.NoError(t, iter.Close(ctx))
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestDiffPartitionExhaustionClosesChild(t *testing.T) {
	ctx := sql.NewEmptyContext()
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	rows := make(chan sql.Row)
	close(rows)
	var source sql.RowIter = prollyDiffIter{rows: rows, errChan: make(chan error), cancel: cancel}
	iter := &diffPartitionRowIter{currentPartition: &DiffPartition{}, currentRowIter: &source}
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, child.Err(), context.Canceled)
	require.NoError(t, iter.Close(ctx))
}
