package events

import (
	"context"
	"path/filepath"
	"testing"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	testLib "github.com/liquidata-inc/dolt/go/libraries/utils/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
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

func testSMHomeDirFunc() (string, error) {
	return homeDir, nil
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

func TestEFInMem(t *testing.T) {
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
			ctx := context.Background()

			fs := filesys.NewInMemFS([]string{tempEvtsDir}, nil, tempEvtsDir)

			// client := NewTestClient()
			// sn := NewSequentialNamer()

			// fbp := NewFileBackedProc(fs, homeDir, doltDir, sn.Name, sn.Check)

			// em := &GrpcEmitter{client}
			// flush := &GrpcEventFlusher{em: em, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

			flushTester := createFlushTester(fs, homeDir, doltDir)

			ces := make([]*eventsapi.ClientEvent, 0)

			for i := 0; i < test.numEvents; i++ {
				ce := &eventsapi.ClientEvent{}
				ces = append(ces, ce)
			}

			assert.Equal(t, len(ces), test.numEvents)

			err := flushTester.Fbp.WriteEvents(testVersion, ces)
			assert.Equal(t, err, nil)

			err = flushTester.Flusher.FlushEvents(ctx)

			assert.Equal(t, err, nil)
			assert.Equal(t, len(flushTester.Client.CES), len(ces))
		})
	}
}

func TestEFLocal(t *testing.T) {
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
			fs := filesys.LocalFS

			dDir := testLib.TestDir(dPath)
			evtDir := filepath.Join(dDir, evtPath)

			fs.MkDirs(evtDir)
			exists, _ := fs.Exists(evtDir)
			assert.Equal(t, exists, true)

			ctx := context.Background()

			flushTester := createFlushTester(fs, "", dPath)

			ces := make([]*eventsapi.ClientEvent, 0)

			for i := 0; i < test.numEvents; i++ {
				ce := &eventsapi.ClientEvent{}
				ces = append(ces, ce)
			}

			err := flushTester.Fbp.WriteEvents(testVersion, ces)
			assert.Equal(t, err, nil)

			err = flushTester.Flusher.FlushEvents(ctx)

			assert.Equal(t, err, nil)
			assert.Equal(t, len(flushTester.Client.CES), len(ces))
		})
	}
}
