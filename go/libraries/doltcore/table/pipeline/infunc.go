package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"time"
)

// InFuncForChannel returns an InFunc that reads off the channel given. Probably belongs in the pipeline package
// eventually.
func InFuncForChannel(cpChan <-chan row.Row) InFunc {
	return func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{}) {
		defer close(ch)

		for {
			select {
			case <-noMoreChan:
				return
			default:
				break
			}

			if p.IsStopping() {
				return
			}

			select {
			case r, ok := <-cpChan:
				if ok {
					ch <- RowWithProps{Row: r, Props: NoProps}
				} else {
					return
				}
			case <-time.After(100 * time.Millisecond):
				// wake up and check stop condition
			}
		}
	}
}
