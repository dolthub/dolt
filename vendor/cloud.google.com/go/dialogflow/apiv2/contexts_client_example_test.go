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
	"cloud.google.com/go/dialogflow/apiv2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
)

func ExampleNewContextsClient() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleContextsClient_ListContexts() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ListContextsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListContexts(ctx, req)
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

func ExampleContextsClient_GetContext() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.GetContextRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetContext(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleContextsClient_CreateContext() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.CreateContextRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateContext(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleContextsClient_UpdateContext() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.UpdateContextRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateContext(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleContextsClient_DeleteContext() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DeleteContextRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteContext(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleContextsClient_DeleteAllContexts() {
	ctx := context.Background()
	c, err := dialogflow.NewContextsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DeleteAllContextsRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteAllContexts(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
