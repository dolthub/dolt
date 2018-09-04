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

func ExampleNewEntityTypesClient() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleEntityTypesClient_ListEntityTypes() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ListEntityTypesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListEntityTypes(ctx, req)
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

func ExampleEntityTypesClient_GetEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.GetEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleEntityTypesClient_CreateEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.CreateEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleEntityTypesClient_UpdateEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.UpdateEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleEntityTypesClient_DeleteEntityType() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.DeleteEntityTypeRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteEntityType(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleEntityTypesClient_BatchUpdateEntityTypes() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchUpdateEntityTypesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchUpdateEntityTypes(ctx, req)
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

func ExampleEntityTypesClient_BatchDeleteEntityTypes() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchDeleteEntityTypesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchDeleteEntityTypes(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleEntityTypesClient_BatchCreateEntities() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchCreateEntitiesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchCreateEntities(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleEntityTypesClient_BatchUpdateEntities() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchUpdateEntitiesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchUpdateEntities(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleEntityTypesClient_BatchDeleteEntities() {
	ctx := context.Background()
	c, err := dialogflow.NewEntityTypesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.BatchDeleteEntitiesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchDeleteEntities(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}
