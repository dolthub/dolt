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

package speech_test

import (
	"io"

	"cloud.google.com/go/speech/apiv1p1beta1"
	"golang.org/x/net/context"
	speechpb "google.golang.org/genproto/googleapis/cloud/speech/v1p1beta1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_Recognize() {
	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &speechpb.RecognizeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Recognize(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_LongRunningRecognize() {
	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &speechpb.LongRunningRecognizeRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.LongRunningRecognize(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_StreamingRecognize() {
	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	stream, err := c.StreamingRecognize(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	go func() {
		reqs := []*speechpb.StreamingRecognizeRequest{
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
