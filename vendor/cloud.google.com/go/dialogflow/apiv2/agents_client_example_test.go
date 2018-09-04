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

func ExampleNewAgentsClient() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleAgentsClient_GetAgent() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.GetAgentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetAgent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleAgentsClient_SearchAgents() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.SearchAgentsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.SearchAgents(ctx, req)
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

func ExampleAgentsClient_TrainAgent() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.TrainAgentRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.TrainAgent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleAgentsClient_ExportAgent() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ExportAgentRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.ExportAgent(ctx, req)
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

func ExampleAgentsClient_ImportAgent() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.ImportAgentRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.ImportAgent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleAgentsClient_RestoreAgent() {
	ctx := context.Background()
	c, err := dialogflow.NewAgentsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dialogflowpb.RestoreAgentRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.RestoreAgent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}
