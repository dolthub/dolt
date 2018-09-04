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

package dataproc_test

import (
	"cloud.google.com/go/dataproc/apiv1beta2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
)

func ExampleNewWorkflowTemplateClient() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleWorkflowTemplateClient_CreateWorkflowTemplate() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.CreateWorkflowTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateWorkflowTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleWorkflowTemplateClient_GetWorkflowTemplate() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.GetWorkflowTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetWorkflowTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleWorkflowTemplateClient_InstantiateWorkflowTemplate() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.InstantiateWorkflowTemplateRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.InstantiateWorkflowTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleWorkflowTemplateClient_UpdateWorkflowTemplate() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.UpdateWorkflowTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateWorkflowTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleWorkflowTemplateClient_ListWorkflowTemplates() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.ListWorkflowTemplatesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListWorkflowTemplates(ctx, req)
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

func ExampleWorkflowTemplateClient_DeleteWorkflowTemplate() {
	ctx := context.Background()
	c, err := dataproc.NewWorkflowTemplateClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.DeleteWorkflowTemplateRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteWorkflowTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
