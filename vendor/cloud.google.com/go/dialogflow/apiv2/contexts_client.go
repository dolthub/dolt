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

package dialogflow

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
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// ContextsCallOptions contains the retry settings for each method of ContextsClient.
type ContextsCallOptions struct {
	ListContexts      []gax.CallOption
	GetContext        []gax.CallOption
	CreateContext     []gax.CallOption
	UpdateContext     []gax.CallOption
	DeleteContext     []gax.CallOption
	DeleteAllContexts []gax.CallOption
}

func defaultContextsClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dialogflow.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultContextsCallOptions() *ContextsCallOptions {
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
	return &ContextsCallOptions{
		ListContexts:      retry[[2]string{"default", "idempotent"}],
		GetContext:        retry[[2]string{"default", "idempotent"}],
		CreateContext:     retry[[2]string{"default", "non_idempotent"}],
		UpdateContext:     retry[[2]string{"default", "non_idempotent"}],
		DeleteContext:     retry[[2]string{"default", "idempotent"}],
		DeleteAllContexts: retry[[2]string{"default", "idempotent"}],
	}
}

// ContextsClient is a client for interacting with Dialogflow API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type ContextsClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	contextsClient dialogflowpb.ContextsClient

	// The call options for this service.
	CallOptions *ContextsCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewContextsClient creates a new contexts client.
//
// A context represents additional information included with user input or with
// an intent returned by the Dialogflow API. Contexts are helpful for
// differentiating user input which may be vague or have a different meaning
// depending on additional details from your application such as user setting
// and preferences, previous user input, where the user is in your application,
// geographic location, and so on.
//
// You can include contexts as input parameters of a
// [DetectIntent][google.cloud.dialogflow.v2.Sessions.DetectIntent] (or
// [StreamingDetectIntent][google.cloud.dialogflow.v2.Sessions.StreamingDetectIntent]) request,
// or as output contexts included in the returned intent.
// Contexts expire when an intent is matched, after the number of DetectIntent
// requests specified by the lifespan_count parameter, or after 10 minutes
// if no intents are matched for a DetectIntent request.
//
// For more information about contexts, see the
// Dialogflow documentation (at https://dialogflow.com/docs/contexts).
func NewContextsClient(ctx context.Context, opts ...option.ClientOption) (*ContextsClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultContextsClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &ContextsClient{
		conn:        conn,
		CallOptions: defaultContextsCallOptions(),

		contextsClient: dialogflowpb.NewContextsClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *ContextsClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *ContextsClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *ContextsClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListContexts returns the list of all contexts in the specified session.
func (c *ContextsClient) ListContexts(ctx context.Context, req *dialogflowpb.ListContextsRequest, opts ...gax.CallOption) *ContextIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListContexts[0:len(c.CallOptions.ListContexts):len(c.CallOptions.ListContexts)], opts...)
	it := &ContextIterator{}
	req = proto.Clone(req).(*dialogflowpb.ListContextsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dialogflowpb.Context, string, error) {
		var resp *dialogflowpb.ListContextsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.contextsClient.ListContexts(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Contexts, resp.NextPageToken, nil
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

// GetContext retrieves the specified context.
func (c *ContextsClient) GetContext(ctx context.Context, req *dialogflowpb.GetContextRequest, opts ...gax.CallOption) (*dialogflowpb.Context, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetContext[0:len(c.CallOptions.GetContext):len(c.CallOptions.GetContext)], opts...)
	var resp *dialogflowpb.Context
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.contextsClient.GetContext(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateContext creates a context.
func (c *ContextsClient) CreateContext(ctx context.Context, req *dialogflowpb.CreateContextRequest, opts ...gax.CallOption) (*dialogflowpb.Context, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateContext[0:len(c.CallOptions.CreateContext):len(c.CallOptions.CreateContext)], opts...)
	var resp *dialogflowpb.Context
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.contextsClient.CreateContext(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateContext updates the specified context.
func (c *ContextsClient) UpdateContext(ctx context.Context, req *dialogflowpb.UpdateContextRequest, opts ...gax.CallOption) (*dialogflowpb.Context, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateContext[0:len(c.CallOptions.UpdateContext):len(c.CallOptions.UpdateContext)], opts...)
	var resp *dialogflowpb.Context
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.contextsClient.UpdateContext(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteContext deletes the specified context.
func (c *ContextsClient) DeleteContext(ctx context.Context, req *dialogflowpb.DeleteContextRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteContext[0:len(c.CallOptions.DeleteContext):len(c.CallOptions.DeleteContext)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.contextsClient.DeleteContext(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// DeleteAllContexts deletes all active contexts in the specified session.
func (c *ContextsClient) DeleteAllContexts(ctx context.Context, req *dialogflowpb.DeleteAllContextsRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteAllContexts[0:len(c.CallOptions.DeleteAllContexts):len(c.CallOptions.DeleteAllContexts)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.contextsClient.DeleteAllContexts(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// ContextIterator manages a stream of *dialogflowpb.Context.
type ContextIterator struct {
	items    []*dialogflowpb.Context
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dialogflowpb.Context, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ContextIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *ContextIterator) Next() (*dialogflowpb.Context, error) {
	var item *dialogflowpb.Context
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ContextIterator) bufLen() int {
	return len(it.items)
}

func (it *ContextIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
