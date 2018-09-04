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

func ExampleNewAlertPolicyClient() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleAlertPolicyClient_ListAlertPolicies() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.ListAlertPoliciesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListAlertPolicies(ctx, req)
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

func ExampleAlertPolicyClient_GetAlertPolicy() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.GetAlertPolicyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetAlertPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleAlertPolicyClient_CreateAlertPolicy() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.CreateAlertPolicyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateAlertPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleAlertPolicyClient_DeleteAlertPolicy() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.DeleteAlertPolicyRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteAlertPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleAlertPolicyClient_UpdateAlertPolicy() {
	ctx := context.Background()
	c, err := monitoring.NewAlertPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &monitoringpb.UpdateAlertPolicyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateAlertPolicy(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
