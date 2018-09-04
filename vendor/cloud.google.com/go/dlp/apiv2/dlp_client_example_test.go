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

package dlp_test

import (
	"cloud.google.com/go/dlp/apiv2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	dlppb "google.golang.org/genproto/googleapis/privacy/dlp/v2"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_InspectContent() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.InspectContentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.InspectContent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_RedactImage() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.RedactImageRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.RedactImage(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeidentifyContent() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.DeidentifyContentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.DeidentifyContent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ReidentifyContent() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ReidentifyContentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ReidentifyContent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListInfoTypes() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ListInfoTypesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ListInfoTypes(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_CreateInspectTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.CreateInspectTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateInspectTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_UpdateInspectTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.UpdateInspectTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateInspectTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_GetInspectTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.GetInspectTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetInspectTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListInspectTemplates() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ListInspectTemplatesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListInspectTemplates(ctx, req)
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

func ExampleClient_DeleteInspectTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.DeleteInspectTemplateRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteInspectTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_CreateDeidentifyTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.CreateDeidentifyTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateDeidentifyTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_UpdateDeidentifyTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.UpdateDeidentifyTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateDeidentifyTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_GetDeidentifyTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.GetDeidentifyTemplateRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetDeidentifyTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListDeidentifyTemplates() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ListDeidentifyTemplatesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListDeidentifyTemplates(ctx, req)
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

func ExampleClient_DeleteDeidentifyTemplate() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.DeleteDeidentifyTemplateRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteDeidentifyTemplate(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_CreateDlpJob() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.CreateDlpJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateDlpJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListDlpJobs() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ListDlpJobsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListDlpJobs(ctx, req)
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

func ExampleClient_GetDlpJob() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.GetDlpJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetDlpJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteDlpJob() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.DeleteDlpJobRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteDlpJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_CancelDlpJob() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.CancelDlpJobRequest{
		// TODO: Fill request struct fields.
	}
	err = c.CancelDlpJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_ListJobTriggers() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.ListJobTriggersRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListJobTriggers(ctx, req)
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

func ExampleClient_GetJobTrigger() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.GetJobTriggerRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetJobTrigger(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteJobTrigger() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.DeleteJobTriggerRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteJobTrigger(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_UpdateJobTrigger() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.UpdateJobTriggerRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateJobTrigger(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_CreateJobTrigger() {
	ctx := context.Background()
	c, err := dlp.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dlppb.CreateJobTriggerRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateJobTrigger(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
