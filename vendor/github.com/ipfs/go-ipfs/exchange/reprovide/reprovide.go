package reprovide

import (
	"context"
	"fmt"
	"time"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	backoff "gx/ipfs/QmPJUtEJsm5YLUWhF6imvyCH8KZXRJa9Wup7FDMwTy5Ufz/backoff"
	routing "gx/ipfs/QmPR2JzfKd9poHx9XBhzoFeBBC31ZM3W5iUPKJZWyaoZZm/go-libp2p-routing"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("reprovider")

//KeyChanFunc is function streaming CIDs to pass to content routing
type KeyChanFunc func(context.Context) (<-chan *cid.Cid, error)
type doneFunc func(error)

type Reprovider struct {
	ctx     context.Context
	trigger chan doneFunc

	// The routing system to provide values through
	rsys routing.ContentRouting

	keyProvider KeyChanFunc
}

// NewReprovider creates new Reprovider instance.
func NewReprovider(ctx context.Context, rsys routing.ContentRouting, keyProvider KeyChanFunc) *Reprovider {
	return &Reprovider{
		ctx:     ctx,
		trigger: make(chan doneFunc),

		rsys:        rsys,
		keyProvider: keyProvider,
	}
}

// Run re-provides keys with 'tick' interval or when triggered
func (rp *Reprovider) Run(tick time.Duration) {
	// dont reprovide immediately.
	// may have just started the daemon and shutting it down immediately.
	// probability( up another minute | uptime ) increases with uptime.
	after := time.After(time.Minute)
	var done doneFunc
	for {
		if tick == 0 {
			after = make(chan time.Time)
		}

		select {
		case <-rp.ctx.Done():
			return
		case done = <-rp.trigger:
		case <-after:
		}

		//'mute' the trigger channel so when `ipfs bitswap reprovide` is called
		//a 'reprovider is already running' error is returned
		unmute := rp.muteTrigger()

		err := rp.Reprovide()
		if err != nil {
			log.Debug(err)
		}

		if done != nil {
			done(err)
		}

		unmute()

		after = time.After(tick)
	}
}

// Reprovide registers all keys given by rp.keyProvider to libp2p content routing
func (rp *Reprovider) Reprovide() error {
	keychan, err := rp.keyProvider(rp.ctx)
	if err != nil {
		return fmt.Errorf("Failed to get key chan: %s", err)
	}
	for c := range keychan {
		op := func() error {
			err := rp.rsys.Provide(rp.ctx, c, true)
			if err != nil {
				log.Debugf("Failed to provide key: %s", err)
			}
			return err
		}

		// TODO: this backoff library does not respect our context, we should
		// eventually work contexts into it. low priority.
		err := backoff.Retry(op, backoff.NewExponentialBackOff())
		if err != nil {
			log.Debugf("Providing failed after number of retries: %s", err)
			return err
		}
	}
	return nil
}

// Trigger starts reprovision process in rp.Run and waits for it
func (rp *Reprovider) Trigger(ctx context.Context) error {
	progressCtx, done := context.WithCancel(ctx)

	var err error
	df := func(e error) {
		err = e
		done()
	}

	select {
	case <-rp.ctx.Done():
		return context.Canceled
	case <-ctx.Done():
		return context.Canceled
	case rp.trigger <- df:
		<-progressCtx.Done()
		return err
	}
}

func (rp *Reprovider) muteTrigger() context.CancelFunc {
	ctx, cf := context.WithCancel(rp.ctx)
	go func() {
		defer cf()
		for {
			select {
			case <-ctx.Done():
				return
			case done := <-rp.trigger:
				done(fmt.Errorf("reprovider is already running"))
			}
		}
	}()

	return cf
}
