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

func ExampleNewUptimeCheckClient() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleUptimeCheckClient_ListUptimeCheckConfigs() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListUptimeCheckConfigsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListUptimeCheckConfigs(ctx, req)
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

func ExampleUptimeCheckClient_GetUptimeCheckConfig() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetUptimeCheckConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetUptimeCheckConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleUptimeCheckClient_CreateUptimeCheckConfig() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.CreateUptimeCheckConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateUptimeCheckConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleUptimeCheckClient_UpdateUptimeCheckConfig() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.UpdateUptimeCheckConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateUptimeCheckConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleUptimeCheckClient_DeleteUptimeCheckConfig() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.DeleteUptimeCheckConfigRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteUptimeCheckConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleUptimeCheckClient_ListUptimeCheckIps() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListUptimeCheckIpsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListUptimeCheckIps(ctx, req)
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
