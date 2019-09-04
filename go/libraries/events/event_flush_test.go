package events

import (
	"context"
	"path/filepath"
	"testing"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type TestClient struct {
	CES []*eventsapi.ClientEvent
}

func (tc *TestClient) LogEvents(ctx context.Context, in *eventsapi.LogEventsRequest, opts ...grpc.CallOption) (*eventsapi.LogEventsResponse, error) {
	for _, evt := range in.Events {

		tc.CES = append(tc.CES, evt)
	}
	return &eventsapi.LogEventsResponse{}, nil
}

func NewTestClient() *TestClient {
	return &TestClient{}
}

func TestEF(t *testing.T) {
	homeDir := "/home/"
	doltDir := filepath.Join(homeDir, ".tempDolt")
	eventsDataDir := filepath.Join(homeDir, doltDir, eventsDir)

	version := "1.0.0"

	tests := []struct {
		name      string
		numEvents int
	}{
		{
			name:      "Flush 0 events",
			numEvents: 0,
		},
		{
			name:      "Flush 100 events",
			numEvents: 100,
		},
		{
			name:      "Flush 1000 events",
			numEvents: 1000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := filesys.NewInMemFS([]string{eventsDataDir}, nil, eventsDataDir)

			client := NewTestClient()
			sn := NewSequentialNamer()

			fbp := NewFileBackedProc(fs, homeDir, doltDir, sn.Name, sn.Check)

			em := &GrpcEmitter{client}
			flush := &EventGrpcFlush{em: em, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

			ces := make([]*eventsapi.ClientEvent, 0)

			for i := 0; i < test.numEvents; i++ {
				ce := &eventsapi.ClientEvent{}
				ces = append(ces, ce)
			}

			assert.Equal(t, len(ces), test.numEvents)

			err := fbp.WriteEvents(version, ces)
			assert.Equal(t, err, nil)

			err = flush.FlushEvents()

			assert.Equal(t, err, nil)
			assert.Equal(t, len(client.CES), len(ces))
		})
	}
}
