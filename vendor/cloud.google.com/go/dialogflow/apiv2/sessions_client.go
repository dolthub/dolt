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
	"cloud.google.com/go/internal/version"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SessionsCallOptions contains the retry settings for each method of SessionsClient.
type SessionsCallOptions struct {
	DetectIntent          []gax.CallOption
	StreamingDetectIntent []gax.CallOption
}

func defaultSessionsClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dialogflow.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultSessionsCallOptions() *SessionsCallOptions {
	retry := map[[2]string][]gax.CallOption{}
	return &SessionsCallOptions{
		DetectIntent:          retry[[2]string{"default", "non_idempotent"}],
		StreamingDetectIntent: retry[[2]string{"default", "non_idempotent"}],
	}
}

// SessionsClient is a client for interacting with Dialogflow API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type SessionsClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	sessionsClient dialogflowpb.SessionsClient

	// The call options for this service.
	CallOptions *SessionsCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewSessionsClient creates a new sessions client.
//
// A session represents an interaction with a user. You retrieve user input
// and pass it to the [DetectIntent][google.cloud.dialogflow.v2.Sessions.DetectIntent] (or
// [StreamingDetectIntent][google.cloud.dialogflow.v2.Sessions.StreamingDetectIntent]) method to determine
// user intent and respond.
func NewSessionsClient(ctx context.Context, opts ...option.ClientOption) (*SessionsClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultSessionsClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &SessionsClient{
		conn:        conn,
		CallOptions: defaultSessionsCallOptions(),

		sessionsClient: dialogflowpb.NewSessionsClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *SessionsClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *SessionsClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *SessionsClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// DetectIntent processes a natural language query and returns structured, actionable data
// as a result. This method is not idempotent, because it may cause contexts
// and session entity types to be updated, which in turn might affect
// results of future queries.
func (c *SessionsClient) DetectIntent(ctx context.Context, req *dialogflowpb.DetectIntentRequest, opts ...gax.CallOption) (*dialogflowpb.DetectIntentResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DetectIntent[0:len(c.CallOptions.DetectIntent):len(c.CallOptions.DetectIntent)], opts...)
	var resp *dialogflowpb.DetectIntentResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.sessionsClient.DetectIntent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// StreamingDetectIntent processes a natural language query in audio format in a streaming fashion
// and returns structured, actionable data as a result. This method is only
// available via the gRPC API (not REST).
func (c *SessionsClient) StreamingDetectIntent(ctx context.Context, opts ...gax.CallOption) (dialogflowpb.Sessions_StreamingDetectIntentClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.StreamingDetectIntent[0:len(c.CallOptions.StreamingDetectIntent):len(c.CallOptions.StreamingDetectIntent)], opts...)
	var resp dialogflowpb.Sessions_StreamingDetectIntentClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.sessionsClient.StreamingDetectIntent(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
