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
	"fmt"
	"path/filepath"
	"testing"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type SequentialNamer struct {
	idx int
}

func NewSequentialNamer() *SequentialNamer {
	return &SequentialNamer{}
}

func (namer *SequentialNamer) Name(bytes []byte) string {
	namer.idx += 1
	name := fmt.Sprintf("%04d%s", namer.idx, evtDataExt)

	return name
}

func (namer *SequentialNamer) Check(data []byte, path string) (bool, error) {
	filename := filepath.Base(path)
	ext := filepath.Ext(filename)

	return ext == evtDataExt, nil
}

func (namer *SequentialNamer) GetIdx() int {
	return namer.idx
}

func TestFBP(t *testing.T) {
	homeDir := "/home/"
	doltDir := filepath.Join(homeDir, ".tempDolt")
	eventsDataDir := filepath.Join(homeDir, doltDir, eventsDir)

	version := "1.0.0"

	tests := []struct {
		name      string
		numEvents int
	}{
		{
			name:      "Save 0 events",
			numEvents: 0,
		},
		{
			name:      "Save 100 events",
			numEvents: 100,
		},
		{
			name:      "Save 1000 events",
			numEvents: 1000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// implementation should support dolt filesys api
			fs := filesys.NewInMemFS([]string{eventsDataDir}, nil, eventsDataDir)

			sn := NewSequentialNamer()

			fbp := NewFileBackedProc(fs, homeDir, doltDir, sn.Name, sn.Check)

			ces := make([]*eventsapi.ClientEvent, 0)

			for i := 0; i < test.numEvents; i++ {
				ce := &eventsapi.ClientEvent{}
				ces = append(ces, ce)
			}

			assert.Equal(t, len(ces), test.numEvents)

			err := fbp.WriteEvents(version, ces)
			assert.Equal(t, err, nil)

			filename := fmt.Sprintf("%04d%s", sn.GetIdx(), evtDataExt)
			path := filepath.Join(eventsDataDir, filename)

			data, err := fs.ReadFile(path)
			if test.numEvents == 0 {
				// we expect no file to be written if events length is less than 1
				assert.NotNil(t, err)

			} else {
				// otherwise we should find the file and be able to parse it into a pb Message
				assert.Equal(t, err, nil)

				req := &eventsapi.LogEventsRequest{}

				err = proto.Unmarshal(data, req)

				assert.Equal(t, err, nil)
				assert.Equal(t, len(ces), len(req.Events))
			}
		})
	}
}
