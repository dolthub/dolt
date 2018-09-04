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

package vision

import (
	visionpb "google.golang.org/genproto/googleapis/cloud/vision/v1p1beta1"
)

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ = fmt.Sprintf
var _ = iterator.Done
var _ = strconv.FormatUint
var _ = time.Now

func TestImageAnnotatorSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
	ctx := context.Background()
	ts := testutil.TokenSource(ctx, DefaultAuthScopes()...)
	if ts == nil {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}

	projectId := testutil.ProjID()
	_ = projectId

	c, err := NewImageAnnotatorClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		t.Fatal(err)
	}

	var gcsImageUri string = "gs://gapic-toolkit/President_Barack_Obama.jpg"
	var source = &visionpb.ImageSource{
		GcsImageUri: gcsImageUri,
	}
	var image = &visionpb.Image{
		Source: source,
	}
	var type_ visionpb.Feature_Type = visionpb.Feature_FACE_DETECTION
	var featuresElement = &visionpb.Feature{
		Type: type_,
	}
	var features = []*visionpb.Feature{featuresElement}
	var requestsElement = &visionpb.AnnotateImageRequest{
		Image:    image,
		Features: features,
	}
	var requests = []*visionpb.AnnotateImageRequest{requestsElement}
	var request = &visionpb.BatchAnnotateImagesRequest{
		Requests: requests,
	}

	if _, err := c.BatchAnnotateImages(ctx, request); err != nil {
		t.Error(err)
	}
}
