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

package dataproc

import (
	"math"
	"time"

	"cloud.google.com/go/internal/version"
	"cloud.google.com/go/longrunning"
	lroauto "cloud.google.com/go/longrunning/autogen"
	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// WorkflowTemplateCallOptions contains the retry settings for each method of WorkflowTemplateClient.
type WorkflowTemplateCallOptions struct {
	CreateWorkflowTemplate      []gax.CallOption
	GetWorkflowTemplate         []gax.CallOption
	InstantiateWorkflowTemplate []gax.CallOption
	UpdateWorkflowTemplate      []gax.CallOption
	ListWorkflowTemplates       []gax.CallOption
	DeleteWorkflowTemplate      []gax.CallOption
}

func defaultWorkflowTemplateClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dataproc.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultWorkflowTemplateCallOptions() *WorkflowTemplateCallOptions {
	retry := map[[2]string][]gax.CallOption{
		{"default", "idempotent"}: {
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.3,
				})
			}),
		},
	}
	return &WorkflowTemplateCallOptions{
		CreateWorkflowTemplate:      retry[[2]string{"default", "non_idempotent"}],
		GetWorkflowTemplate:         retry[[2]string{"default", "idempotent"}],
		InstantiateWorkflowTemplate: retry[[2]string{"default", "non_idempotent"}],
		UpdateWorkflowTemplate:      retry[[2]string{"default", "idempotent"}],
		ListWorkflowTemplates:       retry[[2]string{"default", "idempotent"}],
		DeleteWorkflowTemplate:      retry[[2]string{"default", "idempotent"}],
	}
}

// WorkflowTemplateClient is a client for interacting with Google Cloud Dataproc API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type WorkflowTemplateClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	workflowTemplateClient dataprocpb.WorkflowTemplateServiceClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *WorkflowTemplateCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewWorkflowTemplateClient creates a new workflow template service client.
//
// The API interface for managing Workflow Templates in the
// Cloud Dataproc API.
func NewWorkflowTemplateClient(ctx context.Context, opts ...option.ClientOption) (*WorkflowTemplateClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultWorkflowTemplateClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &WorkflowTemplateClient{
		conn:        conn,
		CallOptions: defaultWorkflowTemplateCallOptions(),

		workflowTemplateClient: dataprocpb.NewWorkflowTemplateServiceClient(conn),
	}
	c.setGoogleClientInfo()

	c.LROClient, err = lroauto.NewOperationsClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		// This error "should not happen", since we are just reusing old connection
		// and never actually need to dial.
		// If this does happen, we could leak conn. However, we cannot close conn:
		// If the user invoked the function with option.WithGRPCConn,
		// we would close a connection that's still in use.
		// TODO(pongad): investigate error conditions.
		return nil, err
	}
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *WorkflowTemplateClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *WorkflowTemplateClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *WorkflowTemplateClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateWorkflowTemplate creates new workflow template.
func (c *WorkflowTemplateClient) CreateWorkflowTemplate(ctx context.Context, req *dataprocpb.CreateWorkflowTemplateRequest, opts ...gax.CallOption) (*dataprocpb.WorkflowTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateWorkflowTemplate[0:len(c.CallOptions.CreateWorkflowTemplate):len(c.CallOptions.CreateWorkflowTemplate)], opts...)
	var resp *dataprocpb.WorkflowTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.workflowTemplateClient.CreateWorkflowTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWorkflowTemplate retrieves the latest workflow template.
//
// Can retrieve previously instantiated template by specifying optional
// version parameter.
func (c *WorkflowTemplateClient) GetWorkflowTemplate(ctx context.Context, req *dataprocpb.GetWorkflowTemplateRequest, opts ...gax.CallOption) (*dataprocpb.WorkflowTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetWorkflowTemplate[0:len(c.CallOptions.GetWorkflowTemplate):len(c.CallOptions.GetWorkflowTemplate)], opts...)
	var resp *dataprocpb.WorkflowTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.workflowTemplateClient.GetWorkflowTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// InstantiateWorkflowTemplate instantiates a template and begins execution.
//
// The returned Operation can be used to track execution of
// workflow by polling
// [operations.get][google.longrunning.Operations.GetOperation].
// The Operation will complete when entire workflow is finished.
//
// The running workflow can be aborted via
// [operations.cancel][google.longrunning.Operations.CancelOperation].
// This will cause any inflight jobs to be cancelled and workflow-owned
// clusters to be deleted.
//
// The [Operation.metadata][google.longrunning.Operation.metadata] will be
// [WorkflowMetadata][google.cloud.dataproc.v1beta2.WorkflowMetadata].
//
// On successful completion,
// [Operation.response][google.longrunning.Operation.response] will be
// [Empty][google.protobuf.Empty].
func (c *WorkflowTemplateClient) InstantiateWorkflowTemplate(ctx context.Context, req *dataprocpb.InstantiateWorkflowTemplateRequest, opts ...gax.CallOption) (*InstantiateWorkflowTemplateOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.InstantiateWorkflowTemplate[0:len(c.CallOptions.InstantiateWorkflowTemplate):len(c.CallOptions.InstantiateWorkflowTemplate)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.workflowTemplateClient.InstantiateWorkflowTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &InstantiateWorkflowTemplateOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// UpdateWorkflowTemplate updates (replaces) workflow template. The updated template
// must contain version that matches the current server version.
func (c *WorkflowTemplateClient) UpdateWorkflowTemplate(ctx context.Context, req *dataprocpb.UpdateWorkflowTemplateRequest, opts ...gax.CallOption) (*dataprocpb.WorkflowTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateWorkflowTemplate[0:len(c.CallOptions.UpdateWorkflowTemplate):len(c.CallOptions.UpdateWorkflowTemplate)], opts...)
	var resp *dataprocpb.WorkflowTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.workflowTemplateClient.UpdateWorkflowTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListWorkflowTemplates lists workflows that match the specified filter in the request.
func (c *WorkflowTemplateClient) ListWorkflowTemplates(ctx context.Context, req *dataprocpb.ListWorkflowTemplatesRequest, opts ...gax.CallOption) *WorkflowTemplateIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListWorkflowTemplates[0:len(c.CallOptions.ListWorkflowTemplates):len(c.CallOptions.ListWorkflowTemplates)], opts...)
	it := &WorkflowTemplateIterator{}
	req = proto.Clone(req).(*dataprocpb.ListWorkflowTemplatesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dataprocpb.WorkflowTemplate, string, error) {
		var resp *dataprocpb.ListWorkflowTemplatesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.workflowTemplateClient.ListWorkflowTemplates(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Templates, resp.NextPageToken, nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.PageSize)
	return it
}

// DeleteWorkflowTemplate deletes a workflow template. It does not cancel in-progress workflows.
func (c *WorkflowTemplateClient) DeleteWorkflowTemplate(ctx context.Context, req *dataprocpb.DeleteWorkflowTemplateRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteWorkflowTemplate[0:len(c.CallOptions.DeleteWorkflowTemplate):len(c.CallOptions.DeleteWorkflowTemplate)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.workflowTemplateClient.DeleteWorkflowTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// WorkflowTemplateIterator manages a stream of *dataprocpb.WorkflowTemplate.
type WorkflowTemplateIterator struct {
	items    []*dataprocpb.WorkflowTemplate
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dataprocpb.WorkflowTemplate, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *WorkflowTemplateIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *WorkflowTemplateIterator) Next() (*dataprocpb.WorkflowTemplate, error) {
	var item *dataprocpb.WorkflowTemplate
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *WorkflowTemplateIterator) bufLen() int {
	return len(it.items)
}

func (it *WorkflowTemplateIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// InstantiateWorkflowTemplateOperation manages a long-running operation from InstantiateWorkflowTemplate.
type InstantiateWorkflowTemplateOperation struct {
	lro *longrunning.Operation
}

// InstantiateWorkflowTemplateOperation returns a new InstantiateWorkflowTemplateOperation from a given name.
// The name must be that of a previously created InstantiateWorkflowTemplateOperation, possibly from a different process.
func (c *WorkflowTemplateClient) InstantiateWorkflowTemplateOperation(name string) *InstantiateWorkflowTemplateOperation {
	return &InstantiateWorkflowTemplateOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *InstantiateWorkflowTemplateOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 10000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *InstantiateWorkflowTemplateOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *InstantiateWorkflowTemplateOperation) Metadata() (*dataprocpb.WorkflowMetadata, error) {
	var meta dataprocpb.WorkflowMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *InstantiateWorkflowTemplateOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *InstantiateWorkflowTemplateOperation) Name() string {
	return op.lro.Name()
}

// Cancel starts asynchronous cancellation on a long-running operation.
// The server makes a best effort to cancel the operation, but success is not guaranteed.
// Clients can use Poll or other methods to check whether the cancellation succeeded or whether the operation
// completed despite cancellation. On successful cancellation, the operation is not deleted;
// instead, op.Poll returns an error with code Canceled.
func (op *InstantiateWorkflowTemplateOperation) Cancel(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Cancel(ctx, opts...)
}

// Delete deletes a long-running operation.
// This method indicates that the client is no longer interested in the operation result.
// It does not cancel the operation.
func (op *InstantiateWorkflowTemplateOperation) Delete(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Delete(ctx, opts...)
}
