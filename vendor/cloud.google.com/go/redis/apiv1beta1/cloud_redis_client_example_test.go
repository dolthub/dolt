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

package redis_test

import (
	"cloud.google.com/go/redis/apiv1beta1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	redispb "google.golang.org/genproto/googleapis/cloud/redis/v1beta1"
)

func ExampleNewCloudRedisClient() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleCloudRedisClient_ListInstances() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &redispb.ListInstancesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListInstances(ctx, req)
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

func ExampleCloudRedisClient_GetInstance() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &redispb.GetInstanceRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetInstance(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleCloudRedisClient_CreateInstance() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &redispb.CreateInstanceRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.CreateInstance(ctx, req)
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

func ExampleCloudRedisClient_UpdateInstance() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &redispb.UpdateInstanceRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.UpdateInstance(ctx, req)
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

func ExampleCloudRedisClient_DeleteInstance() {
	ctx := context.Background()
	c, err := redis.NewCloudRedisClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &redispb.DeleteInstanceRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.DeleteInstance(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	err = op.Wait(ctx)
	// TODO: Handle error.
}
