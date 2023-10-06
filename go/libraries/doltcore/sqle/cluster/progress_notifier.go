// Copyright 2023 Dolthub, Inc.
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

package cluster

import (
	"context"
)

// ProgressNotifier is a way for clients to be notified of successful progress
// which a monotonic agent makes after they register to receive notification by
// taking a callback with |Wait|.
//
// As a monotonic agent implementation, you should call |BeginAttempt()|
// anytime you begin attempting to do work. If that work succeeds, you can call
// |RecordSuccess|. If the work fails, you must call |RecordFailure|.
// |RecordSuccess| makes a later call to |RecordFailure| with the same
// |*Attempt| a no-op, so that the call to |RecordFailure| can be safely placed
// in a defer block.
//
// As a client of the agent, you can call |Wait|, which will return a function
// you can call to block until either progress was made since the call to
// |Wait| or the provided |context.Context| is |Done|. If progress is made, the
// function returned from |Wait| returns |nil|. If the context was canceled, it
// returns |context.Cause(ctx)|.
//
// All accesses to ProgressNotifier should be externally synchronized except
// for calling into the functions returned by |Wait|.
type ProgressNotifier struct {
	chs []chan struct{}
}

type Attempt struct {
	chs []chan struct{}
}

func (p *ProgressNotifier) HasWaiters() bool {
	return len(p.chs) > 0
}

func (p *ProgressNotifier) BeginAttempt() *Attempt {
	chs := p.chs
	p.chs = nil
	return &Attempt{ chs: chs }
}

func (*ProgressNotifier) RecordSuccess(a *Attempt) {
	if a.chs != nil {
		for i := range a.chs {
			close(a.chs[i])
		}
		a.chs = nil
	}
}

func (p *ProgressNotifier) RecordFailure(a *Attempt) {
	if a.chs != nil {
		p.chs = append(p.chs, a.chs...)
	}
}

func (p *ProgressNotifier) Wait() func(context.Context) error {
	if len(p.chs) == 0 {
		p.chs = append(p.chs, make(chan struct{}))
	}
	ch := p.chs[0]
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ch:
			return nil
		}
	}
}
