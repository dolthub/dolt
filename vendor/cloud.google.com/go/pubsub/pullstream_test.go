// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"testing"

	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPullStreamGet(t *testing.T) {
	// Test that we retry on the initial Send call from pullstream.get. We don't do this
	// test with the server in fake_test.go because there's no clear way to get Send
	// to fail from the server.
	t.Parallel()
	for _, test := range []struct {
		desc     string
		errors   []error
		wantCode codes.Code
	}{
		{
			desc:     "nil error",
			errors:   []error{nil},
			wantCode: codes.OK,
		},
		{
			desc:     "non-retryable error",
			errors:   []error{status.Errorf(codes.InvalidArgument, "")},
			wantCode: codes.InvalidArgument,
		},
		{
			desc: "retryable errors",
			errors: []error{
				status.Errorf(codes.Unavailable, "first"),
				status.Errorf(codes.Unavailable, "second"),
				nil,
			},
			wantCode: codes.OK,
		},
	} {
		streamingPull := func(context.Context, ...gax.CallOption) (pb.Subscriber_StreamingPullClient, error) {
			if len(test.errors) == 0 {
				panic("out of errors")
			}
			err := test.errors[0]
			test.errors = test.errors[1:]
			return &testStreamingPullClient{sendError: err}, nil
		}
		ps := newPullStream(context.Background(), streamingPull, "")
		_, err := ps.get(nil)
		if got := status.Code(err); got != test.wantCode {
			t.Errorf("%s: got %s, want %s", test.desc, got, test.wantCode)
		}
	}
}

type testStreamingPullClient struct {
	pb.Subscriber_StreamingPullClient
	sendError error
}

func (c *testStreamingPullClient) Send(*pb.StreamingPullRequest) error { return c.sendError }
