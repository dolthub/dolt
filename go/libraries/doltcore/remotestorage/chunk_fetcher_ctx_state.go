package remotestorage

import (
	"context"
	"io"

	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
)

type CtxState interface {
	Run(context.Context) (CtxState, error)
}

type InitialState struct {
	ReqCh  chan *remotesapi.GetDownloadLocsRequest
	Client remotesapi.ChunkStoreServiceClient
}

type WantsOpenState struct {
	ReqCh   chan *remotesapi.GetDownloadLocsRequest
	Initial []*remotesapi.GetDownloadLocsRequest
	Client  remotesapi.ChunkStoreServiceClient
}

type SendInitialState struct {
	ReqCh    chan *remotesapi.GetDownloadLocsRequest
	Initial  []*remotesapi.GetDownloadLocsRequest
	Client   remotesapi.ChunkStoreServiceClient
	StreamCh chan error
	SendCh   chan *remotesapi.GetDownloadLocsRequest
	RecvCh   chan *remotesapi.GetDownloadLocsResponse
}

func (s *InitialState) Run(ctx context.Context) (CtxState, error) {
	select {
	case req, ok := <-s.ReqCh:
		if !ok {
			return nil, nil
		}
		return &WantsOpenState{
			ReqCh:   s.ReqCh,
			Initial: []*remotesapi.GetDownloadLocsRequest{req},
		}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *WantsOpenState) Run(ctx context.Context) (CtxState, error) {
	eg, ctx := errgroup.WithContext(ctx)
	stream, err := s.Client.StreamDownloadLocations(ctx)
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
	return &SendInitialState{
		ReqCh:    s.ReqCh,
		Initial:  s.Initial,
		Client:   s.Client,
		StreamCh: streamCh,
		SendCh:   sendCh,
		RecvCh:   recvCh,
	}, nil
}

func (s *SendInitialState) Run(ctx context.Context) (CtxState, error) {
	panic("unimplemented")
}
