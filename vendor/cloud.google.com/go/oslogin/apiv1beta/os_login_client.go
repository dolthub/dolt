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

package oslogin

import (
	"time"

	"cloud.google.com/go/internal/version"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	commonpb "google.golang.org/genproto/googleapis/cloud/oslogin/common"
	osloginpb "google.golang.org/genproto/googleapis/cloud/oslogin/v1beta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	DeletePosixAccount []gax.CallOption
	DeleteSshPublicKey []gax.CallOption
	GetLoginProfile    []gax.CallOption
	GetSshPublicKey    []gax.CallOption
	ImportSshPublicKey []gax.CallOption
	UpdateSshPublicKey []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("oslogin.googleapis.com:443"),
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
		DeletePosixAccount: retry[[2]string{"default", "idempotent"}],
		DeleteSshPublicKey: retry[[2]string{"default", "idempotent"}],
		GetLoginProfile:    retry[[2]string{"default", "idempotent"}],
		GetSshPublicKey:    retry[[2]string{"default", "idempotent"}],
		ImportSshPublicKey: retry[[2]string{"default", "idempotent"}],
		UpdateSshPublicKey: retry[[2]string{"default", "idempotent"}],
	}
}

// Client is a client for interacting with Google Cloud OS Login API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type Client struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	client osloginpb.OsLoginServiceClient

	// The call options for this service.
	CallOptions *CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new os login service client.
//
// Cloud OS Login API
//
// The Cloud OS Login API allows you to manage users and their associated SSH
// public keys for logging into virtual machines on Google Cloud Platform.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:        conn,
		CallOptions: defaultCallOptions(),

		client: osloginpb.NewOsLoginServiceClient(conn),
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

// DeletePosixAccount deletes a POSIX account.
func (c *Client) DeletePosixAccount(ctx context.Context, req *osloginpb.DeletePosixAccountRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeletePosixAccount[0:len(c.CallOptions.DeletePosixAccount):len(c.CallOptions.DeletePosixAccount)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeletePosixAccount(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// DeleteSshPublicKey deletes an SSH public key.
func (c *Client) DeleteSshPublicKey(ctx context.Context, req *osloginpb.DeleteSshPublicKeyRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteSshPublicKey[0:len(c.CallOptions.DeleteSshPublicKey):len(c.CallOptions.DeleteSshPublicKey)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// GetLoginProfile retrieves the profile information used for logging in to a virtual machine
// on Google Compute Engine.
func (c *Client) GetLoginProfile(ctx context.Context, req *osloginpb.GetLoginProfileRequest, opts ...gax.CallOption) (*osloginpb.LoginProfile, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetLoginProfile[0:len(c.CallOptions.GetLoginProfile):len(c.CallOptions.GetLoginProfile)], opts...)
	var resp *osloginpb.LoginProfile
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetLoginProfile(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetSshPublicKey retrieves an SSH public key.
func (c *Client) GetSshPublicKey(ctx context.Context, req *osloginpb.GetSshPublicKeyRequest, opts ...gax.CallOption) (*commonpb.SshPublicKey, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetSshPublicKey[0:len(c.CallOptions.GetSshPublicKey):len(c.CallOptions.GetSshPublicKey)], opts...)
	var resp *commonpb.SshPublicKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ImportSshPublicKey adds an SSH public key and returns the profile information. Default POSIX
// account information is set when no username and UID exist as part of the
// login profile.
func (c *Client) ImportSshPublicKey(ctx context.Context, req *osloginpb.ImportSshPublicKeyRequest, opts ...gax.CallOption) (*osloginpb.ImportSshPublicKeyResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ImportSshPublicKey[0:len(c.CallOptions.ImportSshPublicKey):len(c.CallOptions.ImportSshPublicKey)], opts...)
	var resp *osloginpb.ImportSshPublicKeyResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ImportSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateSshPublicKey updates an SSH public key and returns the profile information. This method
// supports patch semantics.
func (c *Client) UpdateSshPublicKey(ctx context.Context, req *osloginpb.UpdateSshPublicKeyRequest, opts ...gax.CallOption) (*commonpb.SshPublicKey, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateSshPublicKey[0:len(c.CallOptions.UpdateSshPublicKey):len(c.CallOptions.UpdateSshPublicKey)], opts...)
	var resp *commonpb.SshPublicKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
