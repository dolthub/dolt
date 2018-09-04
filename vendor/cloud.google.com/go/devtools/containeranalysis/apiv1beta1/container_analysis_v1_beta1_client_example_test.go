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

package containeranalysis_test

import (
	"cloud.google.com/go/devtools/containeranalysis/apiv1beta1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	containeranalysispb "google.golang.org/genproto/googleapis/devtools/containeranalysis/v1beta1"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
)

func ExampleNewContainerAnalysisV1Beta1Client() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleContainerAnalysisV1Beta1Client_SetIamPolicy() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
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

func ExampleContainerAnalysisV1Beta1Client_GetIamPolicy() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
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

func ExampleContainerAnalysisV1Beta1Client_TestIamPermissions() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
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

func ExampleContainerAnalysisV1Beta1Client_GetScanConfig() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &containeranalysispb.GetScanConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetScanConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleContainerAnalysisV1Beta1Client_ListScanConfigs() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &containeranalysispb.ListScanConfigsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListScanConfigs(ctx, req)
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

func ExampleContainerAnalysisV1Beta1Client_UpdateScanConfig() {
	ctx := context.Background()
	c, err := containeranalysis.NewContainerAnalysisV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &containeranalysispb.UpdateScanConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateScanConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
