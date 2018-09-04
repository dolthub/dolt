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

package logging_test

import (
	"cloud.google.com/go/logging/apiv2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
)

func ExampleNewConfigClient() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleConfigClient_ListSinks() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.ListSinksRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListSinks(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleConfigClient_GetSink() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.GetSinkRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetSink(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_CreateSink() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.CreateSinkRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateSink(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_UpdateSink() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.UpdateSinkRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateSink(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_DeleteSink() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.DeleteSinkRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteSink(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleConfigClient_ListExclusions() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.ListExclusionsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListExclusions(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleConfigClient_GetExclusion() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.GetExclusionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetExclusion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_CreateExclusion() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.CreateExclusionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateExclusion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_UpdateExclusion() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.UpdateExclusionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateExclusion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleConfigClient_DeleteExclusion() {
	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.DeleteExclusionRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteExclusion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
