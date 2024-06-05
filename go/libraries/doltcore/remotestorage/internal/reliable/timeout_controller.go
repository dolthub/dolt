// Copyright 2024 Dolthub, Inc.
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

package reliable

import (
	"context"
	"errors"
	"time"
)

// |TimeoutController| is a simple dynamic timeout mechanism which state |open|
// uses to enforce timeouts across it's |Open| and |Send| calls. It is intended
// to |Run| within the same errgroup as a set of operations which need timeouts
// enforced. If a registered timeout triggers before being canceled or updated,
// then its |Run| method will return a non-nil error. Only one timeout is in
// effect at a time. Call |SetTimeout| to set the timeout, and call
// |SetTimeout| with an argument of |0| to cancel the timeout enforcement.
//
// The |TimeoutController| should be |Close|d when the work is done. Its |Run|
// method will then return |nil|.
type TimeoutController struct {
	done    chan struct{}
	timeout chan time.Duration
}

func NewTimeoutController() *TimeoutController {
	return &TimeoutController{
		done:    make(chan struct{}),
		timeout: make(chan time.Duration),
	}
}

func (c *TimeoutController) Close() {
	close(c.done)
}

func (c *TimeoutController) SetTimeout(ctx context.Context, d time.Duration) {
	select {
	case c.timeout <- d:
	case <-ctx.Done():
	}
}

func (c *TimeoutController) Run() error {
	var hasTimeout bool
	var t = time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	for {
		var tCh <-chan time.Time
		if hasTimeout {
			tCh = t.C
		}
		select {
		case <-c.done:
			return nil
		case d := <-c.timeout:
			if hasTimeout && !t.Stop() {
				<-t.C
			}
			if d != 0 {
				hasTimeout = true
				t.Reset(d)
			} else {
				hasTimeout = false
			}
		case <-tCh:
			return errors.New("reliableCallStateMachine RPC timeout")
		}
	}
}
