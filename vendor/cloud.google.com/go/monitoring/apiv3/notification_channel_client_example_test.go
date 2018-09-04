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

package monitoring_test

import (
	"cloud.google.com/go/monitoring/apiv3"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

func ExampleNewNotificationChannelClient() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleNotificationChannelClient_ListNotificationChannelDescriptors() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListNotificationChannelDescriptorsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListNotificationChannelDescriptors(ctx, req)
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

func ExampleNotificationChannelClient_GetNotificationChannelDescriptor() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetNotificationChannelDescriptorRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetNotificationChannelDescriptor(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleNotificationChannelClient_ListNotificationChannels() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListNotificationChannelsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListNotificationChannels(ctx, req)
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

func ExampleNotificationChannelClient_GetNotificationChannel() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetNotificationChannelRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetNotificationChannel(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleNotificationChannelClient_CreateNotificationChannel() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.CreateNotificationChannelRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateNotificationChannel(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleNotificationChannelClient_UpdateNotificationChannel() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.UpdateNotificationChannelRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateNotificationChannel(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleNotificationChannelClient_DeleteNotificationChannel() {
	ctx := context.Background()
	c, err := monitoring.NewNotificationChannelClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.DeleteNotificationChannelRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteNotificationChannel(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}
