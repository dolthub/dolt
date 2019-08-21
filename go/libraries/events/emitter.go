package events

import (
	"io"

	"github.com/golang/protobuf/proto"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

type Emitter interface {
	LogEvents(evts []*eventsapi.ClientEvent) error
}

type NullEmitter struct{}

func (ne NullEmitter) LogEvents(evts []*eventsapi.ClientEvent) error {
	return nil
}

type WriterEmitter struct {
	Wr io.Writer
}

func (we WriterEmitter) LogEvents(evts []*eventsapi.ClientEvent) error {
	for _, evt := range evts {
		err := proto.MarshalText(we.Wr, evt)

		if err != nil {
			return err
		}

		_, err = we.Wr.Write([]byte("\n"))

		if err != nil {
			return err
		}
	}

	return nil
}

type GrpcEmitter struct {
}
