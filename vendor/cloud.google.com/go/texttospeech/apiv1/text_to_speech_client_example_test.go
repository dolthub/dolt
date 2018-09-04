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

package texttospeech_test

import (
	"cloud.google.com/go/texttospeech/apiv1"
	"golang.org/x/net/context"
	texttospeechpb "google.golang.org/genproto/googleapis/cloud/texttospeech/v1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := texttospeech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_ListVoices() {
	ctx := context.Background()
	c, err := texttospeech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &texttospeechpb.ListVoicesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ListVoices(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_SynthesizeSpeech() {
	ctx := context.Background()
	c, err := texttospeech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &texttospeechpb.SynthesizeSpeechRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SynthesizeSpeech(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
