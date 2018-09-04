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

package cloudtasks

import (
	"fmt"
	"math"
	"time"

	"cloud.google.com/go/internal/version"
	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	ListQueues         []gax.CallOption
	GetQueue           []gax.CallOption
	CreateQueue        []gax.CallOption
	UpdateQueue        []gax.CallOption
	DeleteQueue        []gax.CallOption
	PurgeQueue         []gax.CallOption
	PauseQueue         []gax.CallOption
	ResumeQueue        []gax.CallOption
	GetIamPolicy       []gax.CallOption
	SetIamPolicy       []gax.CallOption
	TestIamPermissions []gax.CallOption
	ListTasks          []gax.CallOption
	GetTask            []gax.CallOption
	CreateTask         []gax.CallOption
	DeleteTask         []gax.CallOption
	LeaseTasks         []gax.CallOption
	AcknowledgeTask    []gax.CallOption
	RenewLease         []gax.CallOption
	CancelLease        []gax.CallOption
	RunTask            []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("cloudtasks.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultCallOptions() *CallOptions {
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
	return &CallOptions{
		ListQueues:         retry[[2]string{"default", "idempotent"}],
		GetQueue:           retry[[2]string{"default", "idempotent"}],
		CreateQueue:        retry[[2]string{"default", "non_idempotent"}],
		UpdateQueue:        retry[[2]string{"default", "non_idempotent"}],
		DeleteQueue:        retry[[2]string{"default", "idempotent"}],
		PurgeQueue:         retry[[2]string{"default", "non_idempotent"}],
		PauseQueue:         retry[[2]string{"default", "non_idempotent"}],
		ResumeQueue:        retry[[2]string{"default", "non_idempotent"}],
		GetIamPolicy:       retry[[2]string{"default", "idempotent"}],
		SetIamPolicy:       retry[[2]string{"default", "non_idempotent"}],
		TestIamPermissions: retry[[2]string{"default", "idempotent"}],
		ListTasks:          retry[[2]string{"default", "idempotent"}],
		GetTask:            retry[[2]string{"default", "idempotent"}],
		CreateTask:         retry[[2]string{"default", "non_idempotent"}],
		DeleteTask:         retry[[2]string{"default", "idempotent"}],
		LeaseTasks:         retry[[2]string{"default", "non_idempotent"}],
		AcknowledgeTask:    retry[[2]string{"default", "non_idempotent"}],
		RenewLease:         retry[[2]string{"default", "non_idempotent"}],
		CancelLease:        retry[[2]string{"default", "non_idempotent"}],
		RunTask:            retry[[2]string{"default", "non_idempotent"}],
	}
}

// Client is a client for interacting with Cloud Tasks API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type Client struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	client taskspb.CloudTasksClient

	// The call options for this service.
	CallOptions *CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new cloud tasks client.
//
// Cloud Tasks allows developers to manage the execution of background
// work in their applications.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:        conn,
		CallOptions: defaultCallOptions(),

		client: taskspb.NewCloudTasksClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *Client) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListQueues lists queues.
//
// Queues are returned in lexicographical order.
func (c *Client) ListQueues(ctx context.Context, req *taskspb.ListQueuesRequest, opts ...gax.CallOption) *QueueIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListQueues[0:len(c.CallOptions.ListQueues):len(c.CallOptions.ListQueues)], opts...)
	it := &QueueIterator{}
	req = proto.Clone(req).(*taskspb.ListQueuesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*taskspb.Queue, string, error) {
		var resp *taskspb.ListQueuesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListQueues(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Queues, resp.NextPageToken, nil
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

// GetQueue gets a queue.
func (c *Client) GetQueue(ctx context.Context, req *taskspb.GetQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetQueue[0:len(c.CallOptions.GetQueue):len(c.CallOptions.GetQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateQueue creates a queue.
//
// Queues created with this method allow tasks to live for a maximum of 31
// days. After a task is 31 days old, the task will be deleted regardless of whether
// it was dispatched or not.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine queue.yaml or queue.xml file to manage your queues.
// Read
// Overview of Queue Management and queue.yaml (at https://cloud.google.com/tasks/docs/queue-yaml)
// before using this method.
func (c *Client) CreateQueue(ctx context.Context, req *taskspb.CreateQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateQueue[0:len(c.CallOptions.CreateQueue):len(c.CallOptions.CreateQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateQueue updates a queue.
//
// This method creates the queue if it does not exist and updates
// the queue if it does exist.
//
// Queues created with this method allow tasks to live for a maximum of 31
// days. After a task is 31 days old, the task will be deleted regardless of whether
// it was dispatched or not.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine queue.yaml or queue.xml file to manage your queues.
// Read
// Overview of Queue Management and queue.yaml (at https://cloud.google.com/tasks/docs/queue-yaml)
// before using this method.
func (c *Client) UpdateQueue(ctx context.Context, req *taskspb.UpdateQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "queue.name", req.GetQueue().GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateQueue[0:len(c.CallOptions.UpdateQueue):len(c.CallOptions.UpdateQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteQueue deletes a queue.
//
// This command will delete the queue even if it has tasks in it.
//
// Note: If you delete a queue, a queue with the same name can't be created
// for 7 days.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine queue.yaml or queue.xml file to manage your queues.
// Read
// Overview of Queue Management and queue.yaml (at https://cloud.google.com/tasks/docs/queue-yaml)
// before using this method.
func (c *Client) DeleteQueue(ctx context.Context, req *taskspb.DeleteQueueRequest, opts ...gax.CallOption) error {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteQueue[0:len(c.CallOptions.DeleteQueue):len(c.CallOptions.DeleteQueue)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// PurgeQueue purges a queue by deleting all of its tasks.
//
// All tasks created before this method is called are permanently deleted.
//
// Purge operations can take up to one minute to take effect. Tasks
// might be dispatched before the purge takes effect. A purge is irreversible.
func (c *Client) PurgeQueue(ctx context.Context, req *taskspb.PurgeQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.PurgeQueue[0:len(c.CallOptions.PurgeQueue):len(c.CallOptions.PurgeQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.PurgeQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// PauseQueue pauses the queue.
//
// If a queue is paused then the system will stop dispatching tasks
// until the queue is resumed via
// [ResumeQueue][google.cloud.tasks.v2beta2.CloudTasks.ResumeQueue]. Tasks can still be added
// when the queue is paused. A queue is paused if its
// [state][google.cloud.tasks.v2beta2.Queue.state] is [PAUSED][google.cloud.tasks.v2beta2.Queue.State.PAUSED].
func (c *Client) PauseQueue(ctx context.Context, req *taskspb.PauseQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.PauseQueue[0:len(c.CallOptions.PauseQueue):len(c.CallOptions.PauseQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.PauseQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ResumeQueue resume a queue.
//
// This method resumes a queue after it has been
// [PAUSED][google.cloud.tasks.v2beta2.Queue.State.PAUSED] or
// [DISABLED][google.cloud.tasks.v2beta2.Queue.State.DISABLED]. The state of a queue is stored
// in the queue's [state][google.cloud.tasks.v2beta2.Queue.state]; after calling this method it
// will be set to [RUNNING][google.cloud.tasks.v2beta2.Queue.State.RUNNING].
//
// WARNING: Resuming many high-QPS queues at the same time can
// lead to target overloading. If you are resuming high-QPS
// queues, follow the 500/50/5 pattern described in
// Managing Cloud Tasks Scaling Risks (at https://cloud.google.com/tasks/docs/manage-cloud-task-scaling).
func (c *Client) ResumeQueue(ctx context.Context, req *taskspb.ResumeQueueRequest, opts ...gax.CallOption) (*taskspb.Queue, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ResumeQueue[0:len(c.CallOptions.ResumeQueue):len(c.CallOptions.ResumeQueue)], opts...)
	var resp *taskspb.Queue
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ResumeQueue(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetIamPolicy gets the access control policy for a [Queue][google.cloud.tasks.v2beta2.Queue].
// Returns an empty policy if the resource exists and does not have a policy
// set.
//
// Authorization requires the following
// Google IAM (at https://cloud.google.com/iam) permission on the specified
// resource parent:
//
//   cloudtasks.queues.getIamPolicy
func (c *Client) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", req.GetResource()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetIamPolicy[0:len(c.CallOptions.GetIamPolicy):len(c.CallOptions.GetIamPolicy)], opts...)
	var resp *iampb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetIamPolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SetIamPolicy sets the access control policy for a [Queue][google.cloud.tasks.v2beta2.Queue]. Replaces any existing
// policy.
//
// Note: The Cloud Console does not check queue-level IAM permissions yet.
// Project-level permissions are required to use the Cloud Console.
//
// Authorization requires the following
// Google IAM (at https://cloud.google.com/iam) permission on the specified
// resource parent:
//
//   cloudtasks.queues.setIamPolicy
func (c *Client) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", req.GetResource()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.SetIamPolicy[0:len(c.CallOptions.SetIamPolicy):len(c.CallOptions.SetIamPolicy)], opts...)
	var resp *iampb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.SetIamPolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// TestIamPermissions returns permissions that a caller has on a [Queue][google.cloud.tasks.v2beta2.Queue].
// If the resource does not exist, this will return an empty set of
// permissions, not a [NOT_FOUND][google.rpc.Code.NOT_FOUND] error.
//
// Note: This operation is designed to be used for building permission-aware
// UIs and command-line tools, not for authorization checking. This operation
// may "fail open" without warning.
func (c *Client) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest, opts ...gax.CallOption) (*iampb.TestIamPermissionsResponse, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", req.GetResource()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.TestIamPermissions[0:len(c.CallOptions.TestIamPermissions):len(c.CallOptions.TestIamPermissions)], opts...)
	var resp *iampb.TestIamPermissionsResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.TestIamPermissions(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListTasks lists the tasks in a queue.
//
// By default, only the [BASIC][google.cloud.tasks.v2beta2.Task.View.BASIC] view is retrieved
// due to performance considerations;
// [response_view][google.cloud.tasks.v2beta2.ListTasksRequest.response_view] controls the
// subset of information which is returned.
//
// The tasks may be returned in any order. The ordering may change at any
// time.
func (c *Client) ListTasks(ctx context.Context, req *taskspb.ListTasksRequest, opts ...gax.CallOption) *TaskIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListTasks[0:len(c.CallOptions.ListTasks):len(c.CallOptions.ListTasks)], opts...)
	it := &TaskIterator{}
	req = proto.Clone(req).(*taskspb.ListTasksRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*taskspb.Task, string, error) {
		var resp *taskspb.ListTasksResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListTasks(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Tasks, resp.NextPageToken, nil
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

// GetTask gets a task.
func (c *Client) GetTask(ctx context.Context, req *taskspb.GetTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetTask[0:len(c.CallOptions.GetTask):len(c.CallOptions.GetTask)], opts...)
	var resp *taskspb.Task
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetTask(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateTask creates a task and adds it to a queue.
//
// Tasks cannot be updated after creation; there is no UpdateTask command.
//
//   For [App Engine queues][google.cloud.tasks.v2beta2.AppEngineHttpTarget], the maximum task size is
//   100KB.
//
//   For [pull queues][google.cloud.tasks.v2beta2.PullTarget], the maximum task size is 1MB.
func (c *Client) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateTask[0:len(c.CallOptions.CreateTask):len(c.CallOptions.CreateTask)], opts...)
	var resp *taskspb.Task
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateTask(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteTask deletes a task.
//
// A task can be deleted if it is scheduled or dispatched. A task
// cannot be deleted if it has completed successfully or permanently
// failed.
func (c *Client) DeleteTask(ctx context.Context, req *taskspb.DeleteTaskRequest, opts ...gax.CallOption) error {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteTask[0:len(c.CallOptions.DeleteTask):len(c.CallOptions.DeleteTask)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteTask(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// LeaseTasks leases tasks from a pull queue for
// [lease_duration][google.cloud.tasks.v2beta2.LeaseTasksRequest.lease_duration].
//
// This method is invoked by the worker to obtain a lease. The
// worker must acknowledge the task via
// [AcknowledgeTask][google.cloud.tasks.v2beta2.CloudTasks.AcknowledgeTask] after they have
// performed the work associated with the task.
//
// The [payload][google.cloud.tasks.v2beta2.PullMessage.payload] is intended to store data that
// the worker needs to perform the work associated with the task. To
// return the payloads in the [response][google.cloud.tasks.v2beta2.LeaseTasksResponse], set
// [response_view][google.cloud.tasks.v2beta2.LeaseTasksRequest.response_view] to
// [FULL][google.cloud.tasks.v2beta2.Task.View.FULL].
//
// A maximum of 10 qps of [LeaseTasks][google.cloud.tasks.v2beta2.CloudTasks.LeaseTasks]
// requests are allowed per
// queue. [RESOURCE_EXHAUSTED][google.rpc.Code.RESOURCE_EXHAUSTED]
// is returned when this limit is
// exceeded. [RESOURCE_EXHAUSTED][google.rpc.Code.RESOURCE_EXHAUSTED]
// is also returned when
// [max_tasks_dispatched_per_second][google.cloud.tasks.v2beta2.RateLimits.max_tasks_dispatched_per_second]
// is exceeded.
func (c *Client) LeaseTasks(ctx context.Context, req *taskspb.LeaseTasksRequest, opts ...gax.CallOption) (*taskspb.LeaseTasksResponse, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.LeaseTasks[0:len(c.CallOptions.LeaseTasks):len(c.CallOptions.LeaseTasks)], opts...)
	var resp *taskspb.LeaseTasksResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.LeaseTasks(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// AcknowledgeTask acknowledges a pull task.
//
// The worker, that is, the entity that
// [leased][google.cloud.tasks.v2beta2.CloudTasks.LeaseTasks] this task must call this method
// to indicate that the work associated with the task has finished.
//
// The worker must acknowledge a task within the
// [lease_duration][google.cloud.tasks.v2beta2.LeaseTasksRequest.lease_duration] or the lease
// will expire and the task will become available to be leased
// again. After the task is acknowledged, it will not be returned
// by a later [LeaseTasks][google.cloud.tasks.v2beta2.CloudTasks.LeaseTasks],
// [GetTask][google.cloud.tasks.v2beta2.CloudTasks.GetTask], or
// [ListTasks][google.cloud.tasks.v2beta2.CloudTasks.ListTasks].
func (c *Client) AcknowledgeTask(ctx context.Context, req *taskspb.AcknowledgeTaskRequest, opts ...gax.CallOption) error {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.AcknowledgeTask[0:len(c.CallOptions.AcknowledgeTask):len(c.CallOptions.AcknowledgeTask)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.AcknowledgeTask(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// RenewLease renew the current lease of a pull task.
//
// The worker can use this method to extend the lease by a new
// duration, starting from now. The new task lease will be
// returned in the task's [schedule_time][google.cloud.tasks.v2beta2.Task.schedule_time].
func (c *Client) RenewLease(ctx context.Context, req *taskspb.RenewLeaseRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.RenewLease[0:len(c.CallOptions.RenewLease):len(c.CallOptions.RenewLease)], opts...)
	var resp *taskspb.Task
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RenewLease(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CancelLease cancel a pull task's lease.
//
// The worker can use this method to cancel a task's lease by
// setting its [schedule_time][google.cloud.tasks.v2beta2.Task.schedule_time] to now. This will
// make the task available to be leased to the next caller of
// [LeaseTasks][google.cloud.tasks.v2beta2.CloudTasks.LeaseTasks].
func (c *Client) CancelLease(ctx context.Context, req *taskspb.CancelLeaseRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CancelLease[0:len(c.CallOptions.CancelLease):len(c.CallOptions.CancelLease)], opts...)
	var resp *taskspb.Task
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CancelLease(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RunTask forces a task to run now.
//
// When this method is called, Cloud Tasks will dispatch the task, even if
// the task is already running, the queue has reached its [RateLimits][google.cloud.tasks.v2beta2.RateLimits] or
// is [PAUSED][google.cloud.tasks.v2beta2.Queue.State.PAUSED].
//
// This command is meant to be used for manual debugging. For
// example, [RunTask][google.cloud.tasks.v2beta2.CloudTasks.RunTask] can be used to retry a failed
// task after a fix has been made or to manually force a task to be
// dispatched now.
//
// The dispatched task is returned. That is, the task that is returned
// contains the [status][google.cloud.tasks.v2beta2.Task.status] after the task is dispatched but
// before the task is received by its target.
//
// If Cloud Tasks receives a successful response from the task's
// target, then the task will be deleted; otherwise the task's
// [schedule_time][google.cloud.tasks.v2beta2.Task.schedule_time] will be reset to the time that
// [RunTask][google.cloud.tasks.v2beta2.CloudTasks.RunTask] was called plus the retry delay specified
// in the queue's [RetryConfig][google.cloud.tasks.v2beta2.RetryConfig].
//
// [RunTask][google.cloud.tasks.v2beta2.CloudTasks.RunTask] returns
// [NOT_FOUND][google.rpc.Code.NOT_FOUND] when it is called on a
// task that has already succeeded or permanently failed.
//
// [RunTask][google.cloud.tasks.v2beta2.CloudTasks.RunTask] cannot be called on a
// [pull task][google.cloud.tasks.v2beta2.PullMessage].
func (c *Client) RunTask(ctx context.Context, req *taskspb.RunTaskRequest, opts ...gax.CallOption) (*taskspb.Task, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.RunTask[0:len(c.CallOptions.RunTask):len(c.CallOptions.RunTask)], opts...)
	var resp *taskspb.Task
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RunTask(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// QueueIterator manages a stream of *taskspb.Queue.
type QueueIterator struct {
	items    []*taskspb.Queue
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*taskspb.Queue, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *QueueIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *QueueIterator) Next() (*taskspb.Queue, error) {
	var item *taskspb.Queue
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *QueueIterator) bufLen() int {
	return len(it.items)
}

func (it *QueueIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// TaskIterator manages a stream of *taskspb.Task.
type TaskIterator struct {
	items    []*taskspb.Task
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*taskspb.Task, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *TaskIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *TaskIterator) Next() (*taskspb.Task, error) {
	var item *taskspb.Task
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *TaskIterator) bufLen() int {
	return len(it.items)
}

func (it *TaskIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
