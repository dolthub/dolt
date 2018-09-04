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

package datatransfer_test

import (
	"cloud.google.com/go/bigquery/datatransfer/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	datatransferpb "google.golang.org/genproto/googleapis/cloud/bigquery/datatransfer/v1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_GetDataSource() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.GetDataSourceRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetDataSource(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListDataSources() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.ListDataSourcesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListDataSources(ctx, req)
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

func ExampleClient_CreateTransferConfig() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.CreateTransferConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateTransferConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_UpdateTransferConfig() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.UpdateTransferConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateTransferConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteTransferConfig() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.DeleteTransferConfigRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteTransferConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_GetTransferConfig() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.GetTransferConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetTransferConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListTransferConfigs() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.ListTransferConfigsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTransferConfigs(ctx, req)
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

func ExampleClient_ScheduleTransferRuns() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.ScheduleTransferRunsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ScheduleTransferRuns(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_GetTransferRun() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.GetTransferRunRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetTransferRun(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteTransferRun() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.DeleteTransferRunRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteTransferRun(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_ListTransferRuns() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.ListTransferRunsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTransferRuns(ctx, req)
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

func ExampleClient_ListTransferLogs() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.ListTransferLogsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTransferLogs(ctx, req)
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

func ExampleClient_CheckValidCreds() {
	ctx := context.Background()
	c, err := datatransfer.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datatransferpb.CheckValidCredsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CheckValidCreds(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
