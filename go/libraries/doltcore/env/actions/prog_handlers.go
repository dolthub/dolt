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

package actions

import (
	"context"
	"sync"

	"github.com/dolthub/dolt/go/store/datas/pull"
)

func pullerProgFunc(ctx context.Context, statsCh <-chan pull.Stats) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case <-statsCh:
		default:
		}
	}
}

func progFunc(ctx context.Context, progChan <-chan pull.PullProgress) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case <-progChan:
		default:
		}
	}
}

func NoopRunProgFuncs(ctx context.Context) (*sync.WaitGroup, chan pull.PullProgress, chan pull.Stats) {
	statsCh := make(chan pull.Stats)
	progChan := make(chan pull.PullProgress)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(ctx, progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(ctx, statsCh)
	}()

	return wg, progChan, statsCh
}

func NoopStopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan pull.PullProgress, statsCh chan pull.Stats) {
	cancel()
	close(progChan)
	close(statsCh)
	wg.Wait()
}
