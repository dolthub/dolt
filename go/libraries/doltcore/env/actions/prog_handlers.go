package actions

import (
	"context"
	"sync"

	"github.com/dolthub/dolt/go/store/datas"
)

func pullerProgFunc(ctx context.Context, pullerEventCh <-chan datas.PullerEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case <-pullerEventCh:
		default:
		}
	}
}

func progFunc(ctx context.Context, progChan <-chan datas.PullProgress) {
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

func NoopRunProgFuncs(ctx context.Context) (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent) {
	pullerEventCh := make(chan datas.PullerEvent)
	progChan := make(chan datas.PullProgress)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(ctx, progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(ctx, pullerEventCh)
	}()

	return wg, progChan, pullerEventCh
}

func NoopStopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) {
	cancel()
	close(progChan)
	close(pullerEventCh)
	wg.Wait()
}
