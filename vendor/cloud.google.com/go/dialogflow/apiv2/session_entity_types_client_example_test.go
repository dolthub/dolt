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

func ExampleNewSessionEntityTypesClient() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleSessionEntityTypesClient_ListSessionEntityTypes() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ListSessionEntityTypesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListSessionEntityTypes(ctx, req)
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

func ExampleSessionEntityTypesClient_GetSessionEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.GetSessionEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetSessionEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSessionEntityTypesClient_CreateSessionEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.CreateSessionEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateSessionEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSessionEntityTypesClient_UpdateSessionEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.UpdateSessionEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateSessionEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSessionEntityTypesClient_DeleteSessionEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewSessionEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DeleteSessionEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteSessionEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
