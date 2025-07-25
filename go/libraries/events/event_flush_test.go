// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package events

import (
	"context"
	"path/filepath"
	"testing"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	testLib "github.com/dolthub/dolt/go/libraries/utils/test"
)

var (
	testVersion = "1.0.0"
	homeDir     = "/home/"
	dPath       = ".dolt"
	evtPath     = eventsDir
	doltTestDir = filepath.Join(homeDir, dPath)
	tempEvtsDir = filepath.Join(doltTestDir, evtPath)
)

type TestClient struct {
	CES []*eventsapi.ClientEvent
}

func (tc *TestClient) LogEvents(ctx context.Context, in *eventsapi.LogEventsRequest, opts ...grpc.CallOption) (*eventsapi.LogEventsResponse, error) {
	tc.CES = append(tc.CES, in.Events...)
	return &eventsapi.LogEventsResponse{}, nil
}

func NewTestClient() *TestClient {
	return &TestClient{}
}

type flushTester struct {
	Client  *TestClient
	Fbp     *FileBackedProc
	Flusher *FileFlusher
}

func createFlushTester(fs filesys.Filesys, hdir string, ddir string) *flushTester {
	client := NewTestClient()

	sn := NewSequentialNamer()

	fbp := NewFileBackedProc(fs, hdir, ddir, sn.Name, sn.Check)

	gef := &FileFlusher{emitter: &GrpcEmitter{client}, fbp: fbp}

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
		t.Run(fsName, func(t *testing.T) {
			for _, test := range tests {

				t.Run(test.name, func(t *testing.T) {
					ctx := context.Background()

					var ft *flushTester

					if fsName == "inMemFS" {
						fs := filesys.NewInMemFS([]string{tempEvtsDir}, nil, tempEvtsDir)

						ft = createFlushTester(fs, homeDir, doltTestDir)
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

					assert.NoError(t, err)
					assert.Equal(t, len(ft.Client.CES), len(ces))
				})
			}
		})
	}
}
