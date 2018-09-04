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

package spanner_test

import (
	"io"

	"cloud.google.com/go/spanner/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_CreateSession() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.CreateSessionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateSession(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_GetSession() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.GetSessionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetSession(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListSessions() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.ListSessionsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListSessions(ctx, req)
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

func ExampleClient_DeleteSession() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.DeleteSessionRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteSession(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_ExecuteSql() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.ExecuteSqlRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ExecuteSql(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ExecuteStreamingSql() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.ExecuteSqlRequest{
		// TODO: Fill request struct fields.
	}
	stream, err := c.ExecuteStreamingSql(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleClient_Read() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.ReadRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Read(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_StreamingRead() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.ReadRequest{
		// TODO: Fill request struct fields.
	}
	stream, err := c.StreamingRead(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleClient_BeginTransaction() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.BeginTransactionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.BeginTransaction(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_Commit() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.CommitRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Commit(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_Rollback() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.RollbackRequest{
		// TODO: Fill request struct fields.
	}
	err = c.Rollback(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_PartitionQuery() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.PartitionQueryRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.PartitionQuery(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_PartitionRead() {
	ctx := context.Background()
	c, err := spanner.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &spannerpb.PartitionReadRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.PartitionRead(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
