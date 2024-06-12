// Copyright 2024 Dolthub, Inc.
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

package remotesrv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestGRPCSchemeSelection(t *testing.T) {
	rs := &RemoteChunkStore{
		httpScheme: "http",
	}

	md := metadata.New(nil)
	scheme := rs.getScheme(md)
	assert.Equal(t, scheme, "http")

	md.Append("x-forwarded-proto", "https")
	scheme = rs.getScheme(md)
	assert.Equal(t, scheme, "https")
}
