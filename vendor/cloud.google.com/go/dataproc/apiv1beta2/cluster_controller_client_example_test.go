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

func ExampleNewClusterControllerClient() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClusterControllerClient_CreateCluster() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.CreateClusterRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.CreateCluster(ctx, req)
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

func ExampleClusterControllerClient_UpdateCluster() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.UpdateClusterRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.UpdateCluster(ctx, req)
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

func ExampleClusterControllerClient_DeleteCluster() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.DeleteClusterRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.DeleteCluster(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}

func ExampleClusterControllerClient_GetCluster() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.GetClusterRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetCluster(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClusterControllerClient_ListClusters() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.ListClustersRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListClusters(ctx, req)
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

func ExampleClusterControllerClient_DiagnoseCluster() {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.DiagnoseClusterRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.DiagnoseCluster(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}
