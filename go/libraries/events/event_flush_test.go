package events

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	// "time"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var (
	testVersion = "1.0.0"
	smHomeDir   = "/home/"
	doltDir     = filepath.Join(smHomeDir, ".dolt")
	eDDir       = filepath.Join(doltDir, "eventsData")
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
	return smHomeDir, nil
}

func createSendMetricsTestEnv() *env.DoltEnv {
	fs := filesys.NewInMemFS([]string{eDDir}, nil, eDDir)

	dEnv := env.Load(context.Background(), testSMHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func TestEF(t *testing.T) {
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

			fs := filesys.NewInMemFS([]string{eDDir}, nil, eDDir)

			client := NewTestClient()
			sn := NewSequentialNamer()

			fbp := NewFileBackedProc(fs, smHomeDir, doltDir, sn.Name, sn.Check)

			em := &GrpcEmitter{client}
			flush := &EventGrpcFlush{em: em, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

			ces := make([]*eventsapi.ClientEvent, 0)

			for i := 0; i < test.numEvents; i++ {
				ce := &eventsapi.ClientEvent{}
				ces = append(ces, ce)
			}

			assert.Equal(t, len(ces), test.numEvents)

			err := fbp.WriteEvents(testVersion, ces)
			assert.Equal(t, err, nil)

			err = flush.FlushEvents(ctx)

			assert.Equal(t, err, nil)
			assert.Equal(t, len(client.CES), len(ces))
		})
	}
}

func TestFlushConcurrency(t *testing.T) {
	t.Run("test flushevents concurrency", func(t *testing.T) {
		dEnv := createSendMetricsTestEnv()

		exists, _ := dEnv.FS.Exists(eDDir)
		assert.Equal(t, exists, true)

		// test with deadline?
		ctx := context.Background()

		client := NewTestClient()

		root, err := dEnv.GetUserHomeDir()
		assert.Equal(t, err, nil)

		sn := NewSequentialNamer()
		fbp := NewFileBackedProc(dEnv.FS, root, doltDir, sn.Name, sn.Check)

		em := &GrpcEmitter{client}
		egf := &EventGrpcFlush{em: em, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

		j := 0 // files

		// make events slice with i events
		ces := make([]*eventsapi.ClientEvent, 0)
		for i := 0; i < 10; i++ {
			ce := &eventsapi.ClientEvent{}
			ces = append(ces, ce)
		}
		assert.Equal(t, len(ces), 10)

		// write j files each with i events
		for j = 0; j < 10; j++ {
			err = fbp.WriteEvents(testVersion, ces)
			assert.Equal(t, err, nil)
		}

		// err = egf.FlushEvents(ctx)

		// assert.Equal(t, err, nil)
		// assert.Equal(t, len(client.CES), len(ces)*j)

		// timeout := 100 * time.Millisecond
		// errTimeout := errors.New("timedout")

		// make result err chan and a cancel chan

		var wg sync.WaitGroup

		nilErr := errors.New("nil")

		result := make(chan error)
		// cancel := make(chan struct{})

		// start two processes

		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				err = egf.FlushEvents(ctx)
				if err != nil {
					result <- err
					wg.Done()
				}
				result <- nilErr
				wg.Done()
			}()
		}
		wg.Wait()

		for err := range result {
			assert.Equal(t, err, nil)
		}
	})
}
