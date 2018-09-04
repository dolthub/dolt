// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package dialogflow_test

import (
	"io"

	"cloud.google.com/go/dialogflow/apiv2"
	"golang.org/x/net/context"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
)

func ExampleNewSessionsClient() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleSessionsClient_DetectIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DetectIntentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.DetectIntent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSessionsClient_StreamingDetectIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	stream, err := c.StreamingDetectIntent(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	go func() {
		reqs := []*dialogflowpb.StreamingDetectIntentRequest{
			// TODO: Create requests.
		}
		for _, req := range reqs {
			if err := stream.Send(req); err != nil {
				// TODO: Handle error.
			}
		}
		stream.CloseSend()
	}()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}
