package events

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	testLib "github.com/liquidata-inc/dolt/go/libraries/utils/test"
)

var (
	testVersion = "1.0.0"
	homeDir     = "/home/"
	dPath       = dbfactory.DoltDir
	evtPath     = eventsDir
	doltDir     = filepath.Join(homeDir, dPath)
	tempEvtsDir = filepath.Join(doltDir, evtPath)
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

type flushTester struct {
	Client  *TestClient
	Fbp     *FileBackedProc
	Flusher *GrpcEventFlusher
}

func createFlushTester(fs filesys.Filesys, hdir string, ddir string) *flushTester {
	client := NewTestClient()

	sn := NewSequentialNamer()

	fbp := NewFileBackedProc(fs, hdir, ddir, sn.Name, sn.Check)

	gef := &GrpcEventFlusher{em: &GrpcEmitter{client}, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

	return &flushTester{Client: client, Fbp: fbp, Flusher: gef}
}

func TestEventFlushing(t *testing.T) {
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

	filesystems := []string{"inMemFS", "local"}

	for _, fsName := range filesystems {
		for _, test := range tests {

			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()

				var ft *flushTester

				if fsName == "inMemFS" {
					fs := filesys.NewInMemFS([]string{tempEvtsDir}, nil, tempEvtsDir)

					ft = createFlushTester(fs, homeDir, doltDir)
				} else {
					fs := filesys.LocalFS

					path := filepath.Join(dPath, evtPath)
					dDir := testLib.TestDir(path)

					ft = createFlushTester(fs, "", dDir)
				}

				ces := make([]*eventsapi.ClientEvent, 0)

				for i := 0; i < test.numEvents; i++ {
					ce := &eventsapi.ClientEvent{}
					ces = append(ces, ce)
				}

				assert.Equal(t, len(ces), test.numEvents)

				err := ft.Fbp.WriteEvents(testVersion, ces)
				assert.Equal(t, err, nil)

				err = ft.Flusher.Flush(ctx)

				assert.Equal(t, err, nil)
				assert.Equal(t, len(ft.Client.CES), len(ces))
			})
		}
	}
}
