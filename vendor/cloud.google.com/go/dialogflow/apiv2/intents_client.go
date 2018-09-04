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
	"cloud.google.com/go/longrunning"
	lroauto "cloud.google.com/go/longrunning/autogen"
	"github.com/golang/protobuf/proto"
	structpbpb "github.com/golang/protobuf/ptypes/struct"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// IntentsCallOptions contains the retry settings for each method of IntentsClient.
type IntentsCallOptions struct {
	ListIntents        []gax.CallOption
	GetIntent          []gax.CallOption
	CreateIntent       []gax.CallOption
	UpdateIntent       []gax.CallOption
	DeleteIntent       []gax.CallOption
	BatchUpdateIntents []gax.CallOption
	BatchDeleteIntents []gax.CallOption
}

func defaultIntentsClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dialogflow.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultIntentsCallOptions() *IntentsCallOptions {
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
	return &IntentsCallOptions{
		ListIntents:        retry[[2]string{"default", "idempotent"}],
		GetIntent:          retry[[2]string{"default", "idempotent"}],
		CreateIntent:       retry[[2]string{"default", "non_idempotent"}],
		UpdateIntent:       retry[[2]string{"default", "non_idempotent"}],
		DeleteIntent:       retry[[2]string{"default", "idempotent"}],
		BatchUpdateIntents: retry[[2]string{"default", "non_idempotent"}],
		BatchDeleteIntents: retry[[2]string{"default", "idempotent"}],
	}
}

// IntentsClient is a client for interacting with Dialogflow API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type IntentsClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	intentsClient dialogflowpb.IntentsClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *IntentsCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewIntentsClient creates a new intents client.
//
// An intent represents a mapping between input from a user and an action to
// be taken by your application. When you pass user input to the
// [DetectIntent][google.cloud.dialogflow.v2.Sessions.DetectIntent] (or
// [StreamingDetectIntent][google.cloud.dialogflow.v2.Sessions.StreamingDetectIntent]) method, the
// Dialogflow API analyzes the input and searches
// for a matching intent. If no match is found, the Dialogflow API returns a
// fallback intent (`is_fallback` = true).
//
// You can provide additional information for the Dialogflow API to use to
// match user input to an intent by adding the following to your intent.
//
// *   **Contexts** - provide additional context for intent analysis. For
//     example, if an intent is related to an object in your application that
//     plays music, you can provide a context to determine when to match the
//     intent if the user input is “turn it off”.  You can include a context
//     that matches the intent when there is previous user input of
//     "play music", and not when there is previous user input of
//     "turn on the light".
//
// *   **Events** - allow for matching an intent by using an event name
//     instead of user input. Your application can provide an event name and
//     related parameters to the Dialogflow API to match an intent. For
//     example, when your application starts, you can send a welcome event
//     with a user name parameter to the Dialogflow API to match an intent with
//     a personalized welcome message for the user.
//
// *   **Training phrases** - provide examples of user input to train the
//     Dialogflow API agent to better match intents.
//
// For more information about intents, see the
// [Dialogflow documentation](https://dialogflow.com/docs/intents).
func NewIntentsClient(ctx context.Context, opts ...option.ClientOption) (*IntentsClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultIntentsClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &IntentsClient{
		conn:        conn,
		CallOptions: defaultIntentsCallOptions(),

		intentsClient: dialogflowpb.NewIntentsClient(conn),
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
func (c *IntentsClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *IntentsClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *IntentsClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListIntents returns the list of all intents in the specified agent.
func (c *IntentsClient) ListIntents(ctx context.Context, req *dialogflowpb.ListIntentsRequest, opts ...gax.CallOption) *IntentIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListIntents[0:len(c.CallOptions.ListIntents):len(c.CallOptions.ListIntents)], opts...)
	it := &IntentIterator{}
	req = proto.Clone(req).(*dialogflowpb.ListIntentsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dialogflowpb.Intent, string, error) {
		var resp *dialogflowpb.ListIntentsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.intentsClient.ListIntents(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Intents, resp.NextPageToken, nil
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

// GetIntent retrieves the specified intent.
func (c *IntentsClient) GetIntent(ctx context.Context, req *dialogflowpb.GetIntentRequest, opts ...gax.CallOption) (*dialogflowpb.Intent, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetIntent[0:len(c.CallOptions.GetIntent):len(c.CallOptions.GetIntent)], opts...)
	var resp *dialogflowpb.Intent
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.intentsClient.GetIntent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateIntent creates an intent in the specified agent.
func (c *IntentsClient) CreateIntent(ctx context.Context, req *dialogflowpb.CreateIntentRequest, opts ...gax.CallOption) (*dialogflowpb.Intent, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateIntent[0:len(c.CallOptions.CreateIntent):len(c.CallOptions.CreateIntent)], opts...)
	var resp *dialogflowpb.Intent
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.intentsClient.CreateIntent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateIntent updates the specified intent.
func (c *IntentsClient) UpdateIntent(ctx context.Context, req *dialogflowpb.UpdateIntentRequest, opts ...gax.CallOption) (*dialogflowpb.Intent, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateIntent[0:len(c.CallOptions.UpdateIntent):len(c.CallOptions.UpdateIntent)], opts...)
	var resp *dialogflowpb.Intent
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.intentsClient.UpdateIntent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteIntent deletes the specified intent.
func (c *IntentsClient) DeleteIntent(ctx context.Context, req *dialogflowpb.DeleteIntentRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteIntent[0:len(c.CallOptions.DeleteIntent):len(c.CallOptions.DeleteIntent)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.intentsClient.DeleteIntent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// BatchUpdateIntents updates/Creates multiple intents in the specified agent.
//
// Operation <response: [BatchUpdateIntentsResponse][google.cloud.dialogflow.v2.BatchUpdateIntentsResponse]>
func (c *IntentsClient) BatchUpdateIntents(ctx context.Context, req *dialogflowpb.BatchUpdateIntentsRequest, opts ...gax.CallOption) (*BatchUpdateIntentsOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchUpdateIntents[0:len(c.CallOptions.BatchUpdateIntents):len(c.CallOptions.BatchUpdateIntents)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.intentsClient.BatchUpdateIntents(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchUpdateIntentsOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// BatchDeleteIntents deletes intents in the specified agent.
//
// Operation <response: [google.protobuf.Empty][google.protobuf.Empty]>
func (c *IntentsClient) BatchDeleteIntents(ctx context.Context, req *dialogflowpb.BatchDeleteIntentsRequest, opts ...gax.CallOption) (*BatchDeleteIntentsOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchDeleteIntents[0:len(c.CallOptions.BatchDeleteIntents):len(c.CallOptions.BatchDeleteIntents)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.intentsClient.BatchDeleteIntents(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchDeleteIntentsOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// IntentIterator manages a stream of *dialogflowpb.Intent.
type IntentIterator struct {
	items    []*dialogflowpb.Intent
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dialogflowpb.Intent, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *IntentIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *IntentIterator) Next() (*dialogflowpb.Intent, error) {
	var item *dialogflowpb.Intent
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *IntentIterator) bufLen() int {
	return len(it.items)
}

func (it *IntentIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// BatchDeleteIntentsOperation manages a long-running operation from BatchDeleteIntents.
type BatchDeleteIntentsOperation struct {
	lro *longrunning.Operation
}

// BatchDeleteIntentsOperation returns a new BatchDeleteIntentsOperation from a given name.
// The name must be that of a previously created BatchDeleteIntentsOperation, possibly from a different process.
func (c *IntentsClient) BatchDeleteIntentsOperation(name string) *BatchDeleteIntentsOperation {
	return &BatchDeleteIntentsOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchDeleteIntentsOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 5000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *BatchDeleteIntentsOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchDeleteIntentsOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchDeleteIntentsOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchDeleteIntentsOperation) Name() string {
	return op.lro.Name()
}

// BatchUpdateIntentsOperation manages a long-running operation from BatchUpdateIntents.
type BatchUpdateIntentsOperation struct {
	lro *longrunning.Operation
}

// BatchUpdateIntentsOperation returns a new BatchUpdateIntentsOperation from a given name.
// The name must be that of a previously created BatchUpdateIntentsOperation, possibly from a different process.
func (c *IntentsClient) BatchUpdateIntentsOperation(name string) *BatchUpdateIntentsOperation {
	return &BatchUpdateIntentsOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchUpdateIntentsOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*dialogflowpb.BatchUpdateIntentsResponse, error) {
	var resp dialogflowpb.BatchUpdateIntentsResponse
	if err := op.lro.WaitWithInterval(ctx, &resp, 5000*time.Millisecond, opts...); err != nil {
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
func (op *BatchUpdateIntentsOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*dialogflowpb.BatchUpdateIntentsResponse, error) {
	var resp dialogflowpb.BatchUpdateIntentsResponse
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
func (op *BatchUpdateIntentsOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchUpdateIntentsOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchUpdateIntentsOperation) Name() string {
	return op.lro.Name()
}
