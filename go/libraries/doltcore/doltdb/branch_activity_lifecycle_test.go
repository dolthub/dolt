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

package doltdb

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestBranchActivityTrackerClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := NewBranchActivityTracker(context.Background(), true)
		tracker.RecordReadEvent(context.Background(), "db", "main")
		tracker.RecordWriteEvent(context.Background(), "db", "main")
		synctest.Wait()
		require.Len(t, tracker.readTimes, 1)
		require.Len(t, tracker.writeTimes, 1)
		go tracker.Close()
		tracker.Close()
		// Close must have joined the processor before returning.
		select {
		case <-tracker.done:
		default:
			t.Fatal("tracker processor is still running")
		}
		tracker.Close()
	})
}

func TestBranchActivityTrackerParentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		tracker := NewBranchActivityTracker(ctx, true)
		cancel()
		synctest.Wait()
		select {
		case <-tracker.done:
		default:
			t.Fatal("tracker ignored parent cancellation")
		}
		tracker.Close()
	})
}
