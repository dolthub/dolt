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
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// ClusterControllerCallOptions contains the retry settings for each method of ClusterControllerClient.
type ClusterControllerCallOptions struct {
	CreateCluster   []gax.CallOption
	UpdateCluster   []gax.CallOption
	DeleteCluster   []gax.CallOption
	GetCluster      []gax.CallOption
	ListClusters    []gax.CallOption
	DiagnoseCluster []gax.CallOption
}

func defaultClusterControllerClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dataproc.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultClusterControllerCallOptions() *ClusterControllerCallOptions {
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
	return &ClusterControllerCallOptions{
		CreateCluster:   retry[[2]string{"default", "non_idempotent"}],
		UpdateCluster:   retry[[2]string{"default", "non_idempotent"}],
		DeleteCluster:   retry[[2]string{"default", "idempotent"}],
		GetCluster:      retry[[2]string{"default", "idempotent"}],
		ListClusters:    retry[[2]string{"default", "idempotent"}],
		DiagnoseCluster: retry[[2]string{"default", "non_idempotent"}],
	}
}

// ClusterControllerClient is a client for interacting with Google Cloud Dataproc API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type ClusterControllerClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	clusterControllerClient dataprocpb.ClusterControllerClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *ClusterControllerCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClusterControllerClient creates a new cluster controller client.
//
// The ClusterControllerService provides methods to manage clusters
// of Google Compute Engine instances.
func NewClusterControllerClient(ctx context.Context, opts ...option.ClientOption) (*ClusterControllerClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultClusterControllerClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &ClusterControllerClient{
		conn:        conn,
		CallOptions: defaultClusterControllerCallOptions(),

		clusterControllerClient: dataprocpb.NewClusterControllerClient(conn),
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
func (c *ClusterControllerClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *ClusterControllerClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *ClusterControllerClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateCluster creates a cluster in a project.
func (c *ClusterControllerClient) CreateCluster(ctx context.Context, req *dataprocpb.CreateClusterRequest, opts ...gax.CallOption) (*CreateClusterOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateCluster[0:len(c.CallOptions.CreateCluster):len(c.CallOptions.CreateCluster)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.clusterControllerClient.CreateCluster(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// UpdateCluster updates a cluster in a project.
func (c *ClusterControllerClient) UpdateCluster(ctx context.Context, req *dataprocpb.UpdateClusterRequest, opts ...gax.CallOption) (*UpdateClusterOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateCluster[0:len(c.CallOptions.UpdateCluster):len(c.CallOptions.UpdateCluster)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.clusterControllerClient.UpdateCluster(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// DeleteCluster deletes a cluster in a project.
func (c *ClusterControllerClient) DeleteCluster(ctx context.Context, req *dataprocpb.DeleteClusterRequest, opts ...gax.CallOption) (*DeleteClusterOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteCluster[0:len(c.CallOptions.DeleteCluster):len(c.CallOptions.DeleteCluster)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.clusterControllerClient.DeleteCluster(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &DeleteClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// GetCluster gets the resource representation for a cluster in a project.
func (c *ClusterControllerClient) GetCluster(ctx context.Context, req *dataprocpb.GetClusterRequest, opts ...gax.CallOption) (*dataprocpb.Cluster, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetCluster[0:len(c.CallOptions.GetCluster):len(c.CallOptions.GetCluster)], opts...)
	var resp *dataprocpb.Cluster
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.clusterControllerClient.GetCluster(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListClusters lists all regions/{region}/clusters in a project.
func (c *ClusterControllerClient) ListClusters(ctx context.Context, req *dataprocpb.ListClustersRequest, opts ...gax.CallOption) *ClusterIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListClusters[0:len(c.CallOptions.ListClusters):len(c.CallOptions.ListClusters)], opts...)
	it := &ClusterIterator{}
	req = proto.Clone(req).(*dataprocpb.ListClustersRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dataprocpb.Cluster, string, error) {
		var resp *dataprocpb.ListClustersResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.clusterControllerClient.ListClusters(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Clusters, resp.NextPageToken, nil
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

// DiagnoseCluster gets cluster diagnostic information.
// After the operation completes, the Operation.response field
// contains DiagnoseClusterOutputLocation.
func (c *ClusterControllerClient) DiagnoseCluster(ctx context.Context, req *dataprocpb.DiagnoseClusterRequest, opts ...gax.CallOption) (*DiagnoseClusterOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DiagnoseCluster[0:len(c.CallOptions.DiagnoseCluster):len(c.CallOptions.DiagnoseCluster)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.clusterControllerClient.DiagnoseCluster(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &DiagnoseClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// ClusterIterator manages a stream of *dataprocpb.Cluster.
type ClusterIterator struct {
	items    []*dataprocpb.Cluster
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dataprocpb.Cluster, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ClusterIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *ClusterIterator) Next() (*dataprocpb.Cluster, error) {
	var item *dataprocpb.Cluster
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ClusterIterator) bufLen() int {
	return len(it.items)
}

func (it *ClusterIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// CreateClusterOperation manages a long-running operation from CreateCluster.
type CreateClusterOperation struct {
	lro *longrunning.Operation
}

// CreateClusterOperation returns a new CreateClusterOperation from a given name.
// The name must be that of a previously created CreateClusterOperation, possibly from a different process.
func (c *ClusterControllerClient) CreateClusterOperation(name string) *CreateClusterOperation {
	return &CreateClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *CreateClusterOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*dataprocpb.Cluster, error) {
	var resp dataprocpb.Cluster
	if err := op.lro.WaitWithInterval(ctx, &resp, 10000*time.Millisecond, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *CreateClusterOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*dataprocpb.Cluster, error) {
	var resp dataprocpb.Cluster
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *CreateClusterOperation) Metadata() (*dataprocpb.ClusterOperationMetadata, error) {
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *CreateClusterOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *CreateClusterOperation) Name() string {
	return op.lro.Name()
}

// Delete deletes a long-running operation.
// This method indicates that the client is no longer interested in the operation result.
// It does not cancel the operation.
func (op *CreateClusterOperation) Delete(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Delete(ctx, opts...)
}

// DeleteClusterOperation manages a long-running operation from DeleteCluster.
type DeleteClusterOperation struct {
	lro *longrunning.Operation
}

// DeleteClusterOperation returns a new DeleteClusterOperation from a given name.
// The name must be that of a previously created DeleteClusterOperation, possibly from a different process.
func (c *ClusterControllerClient) DeleteClusterOperation(name string) *DeleteClusterOperation {
	return &DeleteClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *DeleteClusterOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 10000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *DeleteClusterOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *DeleteClusterOperation) Metadata() (*dataprocpb.ClusterOperationMetadata, error) {
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *DeleteClusterOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *DeleteClusterOperation) Name() string {
	return op.lro.Name()
}

// Delete deletes a long-running operation.
// This method indicates that the client is no longer interested in the operation result.
// It does not cancel the operation.
func (op *DeleteClusterOperation) Delete(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Delete(ctx, opts...)
}

// DiagnoseClusterOperation manages a long-running operation from DiagnoseCluster.
type DiagnoseClusterOperation struct {
	lro *longrunning.Operation
}

// DiagnoseClusterOperation returns a new DiagnoseClusterOperation from a given name.
// The name must be that of a previously created DiagnoseClusterOperation, possibly from a different process.
func (c *ClusterControllerClient) DiagnoseClusterOperation(name string) *DiagnoseClusterOperation {
	return &DiagnoseClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *DiagnoseClusterOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 10000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *DiagnoseClusterOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *DiagnoseClusterOperation) Metadata() (*dataprocpb.DiagnoseClusterResults, error) {
	var meta dataprocpb.DiagnoseClusterResults
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *DiagnoseClusterOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *DiagnoseClusterOperation) Name() string {
	return op.lro.Name()
}

// Delete deletes a long-running operation.
// This method indicates that the client is no longer interested in the operation result.
// It does not cancel the operation.
func (op *DiagnoseClusterOperation) Delete(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Delete(ctx, opts...)
}

// UpdateClusterOperation manages a long-running operation from UpdateCluster.
type UpdateClusterOperation struct {
	lro *longrunning.Operation
}

// UpdateClusterOperation returns a new UpdateClusterOperation from a given name.
// The name must be that of a previously created UpdateClusterOperation, possibly from a different process.
func (c *ClusterControllerClient) UpdateClusterOperation(name string) *UpdateClusterOperation {
	return &UpdateClusterOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateClusterOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*dataprocpb.Cluster, error) {
	var resp dataprocpb.Cluster
	if err := op.lro.WaitWithInterval(ctx, &resp, 10000*time.Millisecond, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *UpdateClusterOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*dataprocpb.Cluster, error) {
	var resp dataprocpb.Cluster
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *UpdateClusterOperation) Metadata() (*dataprocpb.ClusterOperationMetadata, error) {
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateClusterOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateClusterOperation) Name() string {
	return op.lro.Name()
}

// Delete deletes a long-running operation.
// This method indicates that the client is no longer interested in the operation result.
// It does not cancel the operation.
func (op *UpdateClusterOperation) Delete(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Delete(ctx, opts...)
}
