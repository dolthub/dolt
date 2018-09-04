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

package errorreporting

import (
	clouderrorreportingpb "google.golang.org/genproto/googleapis/devtools/clouderrorreporting/v1beta1"
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

func TestReportErrorsServiceSmoke(t *testing.T) {
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

	c, err := NewReportErrorsClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		t.Fatal(err)
	}

	var formattedProjectName string = fmt.Sprintf("projects/%s", projectId)
	var message string = "[MESSAGE]"
	var service string = "[SERVICE]"
	var serviceContext = &clouderrorreportingpb.ServiceContext{
		Service: service,
	}
	var filePath string = "path/to/file.lang"
	var lineNumber int32 = 42
	var functionName string = "meaningOfLife"
	var reportLocation = &clouderrorreportingpb.SourceLocation{
		FilePath:     filePath,
		LineNumber:   lineNumber,
		FunctionName: functionName,
	}
	var context_ = &clouderrorreportingpb.ErrorContext{
		ReportLocation: reportLocation,
	}
	var event = &clouderrorreportingpb.ReportedErrorEvent{
		Message:        message,
		ServiceContext: serviceContext,
		Context:        context_,
	}
	var request = &clouderrorreportingpb.ReportErrorEventRequest{
		ProjectName: formattedProjectName,
		Event:       event,
	}

	if _, err := c.ReportErrorEvent(ctx, request); err != nil {
		t.Error(err)
	}
}
