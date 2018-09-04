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

func ExampleNewIntentsClient() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleIntentsClient_ListIntents() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ListIntentsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListIntents(ctx, req)
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

func ExampleIntentsClient_GetIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.GetIntentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetIntent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleIntentsClient_CreateIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.CreateIntentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateIntent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleIntentsClient_UpdateIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.UpdateIntentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateIntent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleIntentsClient_DeleteIntent() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DeleteIntentRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteIntent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleIntentsClient_BatchUpdateIntents() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchUpdateIntentsRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchUpdateIntents(ctx, req)
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

func ExampleIntentsClient_BatchDeleteIntents() {
	ctx := context.Background()
	c, err := dialogflow.NewIntentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchDeleteIntentsRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchDeleteIntents(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}
