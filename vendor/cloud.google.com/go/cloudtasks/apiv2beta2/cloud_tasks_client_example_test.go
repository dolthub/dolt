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

package cloudtasks_test

import (
	"cloud.google.com/go/cloudtasks/apiv2beta2"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_ListQueues() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.ListQueuesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListQueues(ctx, req)
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

func ExampleClient_GetQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.GetQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_CreateQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.CreateQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_UpdateQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.UpdateQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.DeleteQueueRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_PurgeQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.PurgeQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.PurgeQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_PauseQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.PauseQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.PauseQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ResumeQueue() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.ResumeQueueRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.ResumeQueue(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_GetIamPolicy() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &iampb.GetIamPolicyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetIamPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_SetIamPolicy() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &iampb.SetIamPolicyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SetIamPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_TestIamPermissions() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &iampb.TestIamPermissionsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.TestIamPermissions(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListTasks() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.ListTasksRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTasks(ctx, req)
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

func ExampleClient_GetTask() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.GetTaskRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetTask(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_CreateTask() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.CreateTaskRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateTask(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteTask() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.DeleteTaskRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteTask(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_LeaseTasks() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.LeaseTasksRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.LeaseTasks(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_AcknowledgeTask() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.AcknowledgeTaskRequest{
		// TODO: Fill request struct fields.
	}
	err = c.AcknowledgeTask(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_RenewLease() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.RenewLeaseRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.RenewLease(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_CancelLease() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.CancelLeaseRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CancelLease(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_RunTask() {
	ctx := context.Background()
	c, err := cloudtasks.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &taskspb.RunTaskRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.RunTask(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
