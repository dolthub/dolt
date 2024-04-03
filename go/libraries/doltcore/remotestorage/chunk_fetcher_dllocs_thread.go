package remotestorage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage/internal/circbuff"
)

// The stream download locations thread is a state machine which converts
// incoming |remotesapi.GetDownloadLocsRequest| requests into
// |[]*remotesapi.DownloadLoc| response messages.
type StreamDownloadLocsThread struct {
	reqCh chan *remotesapi.GetDownloadLocsRequest
	resCh chan []*remotesapi.DownloadLoc

	// These are buffered messages that need to be sent once the connection
	// is opened.
	initial []*remotesapi.GetDownloadLocsRequest
	// The index from which we are sending initial.
	initialIdx int

	// These are inflight messages that might need to be sent if the
	// connection fails --- they will either be thrown away or will end up
	// in |initial|.
	inflight circbuff.Buff[*remotesapi.GetDownloadLocsRequest]

	deliverReq  *remotesapi.GetDownloadLocsRequest
	deliverResp *remotesapi.GetDownloadLocsResponse

	backoffDuration time.Duration
	bo              backoff.BackOff

	client remotesapi.ChunkStoreServiceClient

	sendCh   chan *remotesapi.GetDownloadLocsRequest
	recvCh   chan *remotesapi.GetDownloadLocsResponse
	streamCh chan error
}

func (t *StreamDownloadLocsThread) Run(ctx context.Context) error {
	var curState CtxStateFunc = t.InitialState
	for {
		nextState, err := curState(ctx)
		if err != nil {
			nextState, err = t.processError(err)
			if err != nil {
				return err
			}
		}
		if nextState == nil {
			close(t.resCh)
			return nil
		}
		curState = nextState
	}
}

// In the initial state, we wait for an incoming request.
// If we get one, we transition to the WantsOpen state with an initial request
// list of just that request.
func (t *StreamDownloadLocsThread) InitialState(ctx context.Context) (CtxStateFunc, error) {
	select {
	case req, ok := <-t.reqCh:
		if !ok {
			return nil, nil
		}
		t.initial = append(t.initial, req)
		return t.WantsOpenWithRead, nil
	case <-ctx.Done():
		return t.ctxError(ctx)
	}
}

func (t *StreamDownloadLocsThread) WantsOpenWithRead(ctx context.Context) (CtxStateFunc, error) {
	t.initialIdx = 0
	for t.inflight.Len() > 0 {
		t.initial = append(t.initial, t.inflight.Front())
		t.inflight.Pop()
	}
	t.deliverResp = nil
	t.deliverReq = nil

	eg, ctx := errgroup.WithContext(ctx)
	stream, err := t.client.StreamDownloadLocations(ctx)
	if err != nil {
		return nil, err
	}
	sendCh := make(chan *remotesapi.GetDownloadLocsRequest)
	recvCh := make(chan *remotesapi.GetDownloadLocsResponse)
	streamCh := make(chan error)
	eg.Go(func() error {
		for {
			select {
			case msg, ok := <-sendCh:
				if !ok {
					return stream.CloseSend()
				}
				err = stream.Send(msg)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	eg.Go(func() error {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				close(recvCh)
				return nil
			}
			if err != nil {
				return err
			}
			select {
			case recvCh <- msg:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	go func() {
		streamCh <- eg.Wait()
	}()
	t.sendCh = sendCh
	t.recvCh = recvCh
	t.streamCh = streamCh
	return t.OpenForInitialSend, nil
}

func (t *StreamDownloadLocsThread) OpenForInitialSend(ctx context.Context) (CtxStateFunc, error) {
	if len(t.initial) >= t.initialIdx {
		t.initial = nil
		t.initialIdx = 0
		return t.OpenWithRead, nil
	}
	toSend := t.initial[t.initialIdx]
	thisRecvCh := t.recvCh
	var thisResCh chan []*remotesapi.DownloadLoc
	var thisRes []*remotesapi.DownloadLoc
	if t.deliverResp != nil {
		thisRecvCh = nil
		thisResCh = t.resCh
		thisRes = t.deliverResp.Locs
	}

	// TODO: Timout on thisResCh - transition to a state where we shutdown and hold onto t.deliverResp and can spin back up when responses are deliverable.

	select {
	case t.sendCh <- toSend:
		t.initialIdx += 1
		return t.OpenForInitialSend, nil
	case recv, ok := <-thisRecvCh:
		if !ok {
			// In theory this should never happen...
			err := <-t.streamCh
			return t.streamError(err)
		}
		t.deliverResp = recv
		return t.OpenForInitialSend, nil
	case thisResCh <- thisRes:
		t.deliverResp = nil
		t.initial = t.initial[1:]
		t.initialIdx -= 1
		return t.OpenForInitialSend, nil
	case err := <-t.streamCh:
		return t.streamError(err)
	case <-ctx.Done():
		return t.ctxError(ctx)
	}
}

func (t *StreamDownloadLocsThread) OpenWithRead(ctx context.Context) (CtxStateFunc, error) {
	thisRecvCh := t.recvCh
	var thisResCh chan []*remotesapi.DownloadLoc
	var thisRes []*remotesapi.DownloadLoc
	if t.deliverResp != nil {
		thisRecvCh = nil
		thisResCh = t.resCh
		thisRes = t.deliverResp.Locs
	}

	thisReqCh := t.reqCh
	var thisSendCh chan *remotesapi.GetDownloadLocsRequest
	var thisSend *remotesapi.GetDownloadLocsRequest
	if t.deliverReq != nil {
		thisReqCh = nil
		thisSendCh = t.sendCh
		thisSend = t.deliverReq
	}

	// TODO: Timeout on thisReqCh - transition to a state where we shutdown and go back to initial state.

	// TODO: Timout on thisResCh - transition to a state where we shutdown and hold onto t.deliverResp and can spin back up when responses are deliverable.

	// TODO: There is a race here with streamCh and delivering thisResCh --- if we have this Res != nil, we should not be selecting from streamCh either...

	select {
	case req, ok := <-thisReqCh:
		if !ok {
			t.reqCh = nil
			close(t.sendCh)
			return t.OpenWithRead, nil
		}
		t.inflight.Push(req)
		t.deliverReq = req
		return t.OpenWithRead, nil
	case thisSendCh <- thisSend:
		t.deliverReq = nil
		return t.OpenWithRead, nil
	case recv, ok := <-thisRecvCh:
		if !ok {
			err := <-t.streamCh
			return t.streamError(err)
		}
		t.deliverResp = recv
		return t.OpenWithRead, nil
	case thisResCh <- thisRes:
		t.deliverResp = nil
		t.inflight.Pop()
		return t.OpenWithRead, nil
	case err := <-t.streamCh:
		return t.streamError(err)
	case <-ctx.Done():
		return t.ctxError(ctx)
	}
}

// In an Open state, we have the following cases:
//
//   1) Receive a new message to send to the reqCh or the initial reqs.
//
//      If we don't already have one of these, then we can either get one from the initial reqs, or we can get one from the reqCh.
//
//      If the message was from initial reqs, we can increase initialIdx. If the message was from reqCh, we Push onto inflight.
//
//      Either way, if we do get one, we now have a message to transfer to the sendCh. We won't try to get a new message if we already have one for the sendCh.
//
//   1a) Receive a close from the reqCh.
//
//       If we do this, we close the sendCh and we nil out our reqCh, since we will not receive from it anymore.
//
//   2) Send a message to the send thread.
//
//      We can clear our pending message for sendCh in this case.
//
//      We do not need to update initialIdx or inflight here.
//
//   3) Receive a message from the recvCh.
//
//      We only do this if we are not trying to deliver a messsage to resCh.
//
//      The received message will become our pending message which we are trying to deliver to resCh.
//
//      We do not need to update initialIdx or inflight here.
//
//   3b) Receive a close from the recvCh.
//
//       If we receive a close, it means we must have seen a close from reqCh and we must have closed sendCh.
//
//       We do not have a pending response to deliver, so we can just return streamError(nil) here.
//
//   3c) Receive an error or a nil from streamCh.
//
//       In cases where we are receiving from recvCh, we should also receive from streamCh.
//
//       If the stream closes successfully, the nil error on streamCh will race with close of recvCh.
//
//       If a thread encounters an error, it will be delivered here and return streamError(err) will surface it.
//
//   4) Deliver a recv to resCh.
//
//      We only do this if we have a pending response which we are trying to deliver.
//
//      When we do this, we either update initialIdx or inflight.

type OpenState struct {
	ReqCh  chan *remotesapi.GetDownloadLocsRequest
	GotReq func(*remotesapi.GetDownloadLocsRequest)

	SendCh  chan *remotesapi.GetDownloadLocsRequest
	SendReq *remotesapi.GetDownloadLocsRequest
	SentReq func()

	StreamCh chan error
	RecvCh   chan *remotesapi.GetDownloadLocsResponse
	GotResp  func(r *remotesapi.GetDownloadLocsResponse)

	LocsCh        chan []*remotesapi.DownloadLoc
	LocsResp      []*remotesapi.DownloadLoc
	DeliveredResp func()
}

func (t *StreamDownloadLocsThread) Open(ctx context.Context) (CtxStateFunc, error) {
	s := t.GetOpenState()

	select {
	case s.SendCh <- s.SendReq:
		s.SentReq()
		return t.Open, nil
	case req, ok := <-s.ReqCh:
		if !ok {
			t.reqCh = nil
			close(t.sendCh)
			return t.Open, nil
		}
		s.GotReq(req)
		return t.Open, nil
	case s.LocsCh <- s.LocsResp:
		s.DeliveredResp()
		return t.Open, nil
	case recv, ok := <-s.RecvCh:
		if !ok {
			err := <-s.StreamCh
			return t.streamError(err)
		}
		s.GotResp(recv)
		return t.Open, nil
	case err := <-s.StreamCh:
		return t.streamError(err)
	case <-ctx.Done():
		return t.ctxError(ctx)
	}
}

func (t *StreamDownloadLocsThread) GetOpenState() *OpenState {
	ret := new(OpenState)
	if t.deliverReq != nil {
		ret.SendCh = t.sendCh
		ret.SendReq = t.deliverReq
		ret.SentReq = func() {
			t.deliverReq = nil
		}
	} else {
		if t.initialIdx < len(t.initial) {
			ret.ReqCh = make(chan *remotesapi.GetDownloadLocsRequest, 1)
			ret.ReqCh <- t.initial[t.initialIdx]
			ret.GotReq = func(r *remotesapi.GetDownloadLocsRequest) {
				t.deliverReq = r
				t.initialIdx += 1
			}
		} else {
			ret.ReqCh = t.reqCh
			ret.GotReq = func(r *remotesapi.GetDownloadLocsRequest) {
				t.deliverReq = r
				t.inflight.Push(r)
			}
		}
	}

	if t.deliverResp != nil {
		ret.LocsCh = t.resCh
		ret.LocsResp = t.deliverResp.Locs
		ret.DeliveredResp = func() {
			t.deliverResp = nil
			if t.initialIdx < len(t.initial) {
				t.initialIdx += 1
			} else {
				t.inflight.Pop()
			}
		}
	} else {
		ret.StreamCh = t.streamCh
		ret.RecvCh = t.recvCh
		ret.GotResp = func(r *remotesapi.GetDownloadLocsResponse) {
			t.deliverResp = r
		}
	}

	return ret
}

func (t *StreamDownloadLocsThread) BackoffWantsOpenWithRead(ctx context.Context) (CtxStateFunc, error) {
	select {
	case <-ctx.Done():
		return t.ctxError(ctx)
	case <-time.After(t.backoffDuration):
		return t.WantsOpenWithRead, nil
	}
}

func (t *StreamDownloadLocsThread) processError(err error) (CtxStateFunc, error) {
	err = processGrpcErr(err)
	pe := new(backoff.PermanentError)
	if errors.As(err, &pe) {
		return nil, pe.Err
	}
	t.backoffDuration = t.bo.NextBackOff()
	if t.backoffDuration == backoff.Stop {
		return nil, err
	}
	return t.BackoffWantsOpenWithRead, nil
}

func (t *StreamDownloadLocsThread) ctxError(ctx context.Context) (CtxStateFunc, error) {
	if t.streamCh != nil {
		<-t.streamCh
		t.sendCh, t.recvCh, t.streamCh = nil, nil, nil
	}
	return nil, context.Cause(ctx)
}

func (t *StreamDownloadLocsThread) streamError(err error) (CtxStateFunc, error) {
	t.sendCh, t.recvCh, t.streamCh = nil, nil, nil
	return nil, err
}
