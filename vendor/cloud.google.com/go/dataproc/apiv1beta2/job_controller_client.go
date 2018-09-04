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
	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// JobControllerCallOptions contains the retry settings for each method of JobControllerClient.
type JobControllerCallOptions struct {
	SubmitJob []gax.CallOption
	GetJob    []gax.CallOption
	ListJobs  []gax.CallOption
	UpdateJob []gax.CallOption
	CancelJob []gax.CallOption
	DeleteJob []gax.CallOption
}

func defaultJobControllerClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dataproc.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultJobControllerCallOptions() *JobControllerCallOptions {
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
	return &JobControllerCallOptions{
		SubmitJob: retry[[2]string{"default", "non_idempotent"}],
		GetJob:    retry[[2]string{"default", "idempotent"}],
		ListJobs:  retry[[2]string{"default", "idempotent"}],
		UpdateJob: retry[[2]string{"default", "non_idempotent"}],
		CancelJob: retry[[2]string{"default", "non_idempotent"}],
		DeleteJob: retry[[2]string{"default", "idempotent"}],
	}
}

// JobControllerClient is a client for interacting with Google Cloud Dataproc API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type JobControllerClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	jobControllerClient dataprocpb.JobControllerClient

	// The call options for this service.
	CallOptions *JobControllerCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewJobControllerClient creates a new job controller client.
//
// The JobController provides methods to manage jobs.
func NewJobControllerClient(ctx context.Context, opts ...option.ClientOption) (*JobControllerClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultJobControllerClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &JobControllerClient{
		conn:        conn,
		CallOptions: defaultJobControllerCallOptions(),

		jobControllerClient: dataprocpb.NewJobControllerClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *JobControllerClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *JobControllerClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *JobControllerClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// SubmitJob submits a job to a cluster.
func (c *JobControllerClient) SubmitJob(ctx context.Context, req *dataprocpb.SubmitJobRequest, opts ...gax.CallOption) (*dataprocpb.Job, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.SubmitJob[0:len(c.CallOptions.SubmitJob):len(c.CallOptions.SubmitJob)], opts...)
	var resp *dataprocpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.jobControllerClient.SubmitJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetJob gets the resource representation for a job in a project.
func (c *JobControllerClient) GetJob(ctx context.Context, req *dataprocpb.GetJobRequest, opts ...gax.CallOption) (*dataprocpb.Job, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetJob[0:len(c.CallOptions.GetJob):len(c.CallOptions.GetJob)], opts...)
	var resp *dataprocpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.jobControllerClient.GetJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListJobs lists regions/{region}/jobs in a project.
func (c *JobControllerClient) ListJobs(ctx context.Context, req *dataprocpb.ListJobsRequest, opts ...gax.CallOption) *JobIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListJobs[0:len(c.CallOptions.ListJobs):len(c.CallOptions.ListJobs)], opts...)
	it := &JobIterator{}
	req = proto.Clone(req).(*dataprocpb.ListJobsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dataprocpb.Job, string, error) {
		var resp *dataprocpb.ListJobsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.jobControllerClient.ListJobs(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Jobs, resp.NextPageToken, nil
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

// UpdateJob updates a job in a project.
func (c *JobControllerClient) UpdateJob(ctx context.Context, req *dataprocpb.UpdateJobRequest, opts ...gax.CallOption) (*dataprocpb.Job, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateJob[0:len(c.CallOptions.UpdateJob):len(c.CallOptions.UpdateJob)], opts...)
	var resp *dataprocpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.jobControllerClient.UpdateJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CancelJob starts a job cancellation request. To access the job resource
// after cancellation, call
// regions/{region}/jobs.list (at /dataproc/docs/reference/rest/v1beta2/projects.regions.jobs/list) or
// regions/{region}/jobs.get (at /dataproc/docs/reference/rest/v1beta2/projects.regions.jobs/get).
func (c *JobControllerClient) CancelJob(ctx context.Context, req *dataprocpb.CancelJobRequest, opts ...gax.CallOption) (*dataprocpb.Job, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CancelJob[0:len(c.CallOptions.CancelJob):len(c.CallOptions.CancelJob)], opts...)
	var resp *dataprocpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.jobControllerClient.CancelJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteJob deletes the job from the project. If the job is active, the delete fails,
// and the response returns FAILED_PRECONDITION.
func (c *JobControllerClient) DeleteJob(ctx context.Context, req *dataprocpb.DeleteJobRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteJob[0:len(c.CallOptions.DeleteJob):len(c.CallOptions.DeleteJob)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.jobControllerClient.DeleteJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// JobIterator manages a stream of *dataprocpb.Job.
type JobIterator struct {
	items    []*dataprocpb.Job
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dataprocpb.Job, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *JobIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *JobIterator) Next() (*dataprocpb.Job, error) {
	var item *dataprocpb.Job
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *JobIterator) bufLen() int {
	return len(it.items)
}

func (it *JobIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
