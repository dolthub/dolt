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

package index

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// WaitForKeylessIndexIterGoroutine is a test util that waits up to timeout for a prollyKeylessIndexIter's
// background queueRows goroutine to return, and reports whether it did.
func WaitForKeylessIndexIterGoroutine(iter sql.RowIter, timeout time.Duration) bool {
	p, ok := iter.(prollyKeylessIndexIter)
	if !ok {
		return false
	}
	done := make(chan struct{})
	go func() {
		_ = p.eg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
