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
	"cloud.google.com/go/dataproc/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
)

func ExampleNewJobControllerClient() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleJobControllerClient_SubmitJob() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.SubmitJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SubmitJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleJobControllerClient_GetJob() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.GetJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleJobControllerClient_ListJobs() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.ListJobsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListJobs(ctx, req)
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

func ExampleJobControllerClient_UpdateJob() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.UpdateJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleJobControllerClient_CancelJob() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.CancelJobRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CancelJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleJobControllerClient_DeleteJob() {
	ctx := context.Background()
	c, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.DeleteJobRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteJob(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
