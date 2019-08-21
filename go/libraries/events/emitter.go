package events

import (
	"io"

	"github.com/golang/protobuf/proto"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

// Emitter is an interface used for processing a batch of events
type Emitter interface {
	// LogEvents takes a batch of events and processes them
	LogEvents(evts []*eventsapi.ClientEvent) error
}

// NullEmitter is an emitter that drops events
type NullEmitter struct{}

// LogEvents takes a batch of events and processes them.  In this case it just drops them
func (ne NullEmitter) LogEvents(evts []*eventsapi.ClientEvent) error {
	return nil
}

// WriterEmitter is an emitter that writes the text encoding of the events to it's writer
type WriterEmitter struct {
	// Wr the writer to log events to
	Wr io.Writer
}

// LogEvents takes a batch of events and processes them.  In this case the text encoding of the events is written to
// the writer
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

// GrpcEmitter sends events to a GRPC service implementing the eventsapi
type GrpcEmitter struct {
}
