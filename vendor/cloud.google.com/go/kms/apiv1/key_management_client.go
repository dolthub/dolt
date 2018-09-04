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

package kms

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
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// KeyManagementCallOptions contains the retry settings for each method of KeyManagementClient.
type KeyManagementCallOptions struct {
	ListKeyRings                  []gax.CallOption
	ListCryptoKeys                []gax.CallOption
	ListCryptoKeyVersions         []gax.CallOption
	GetKeyRing                    []gax.CallOption
	GetCryptoKey                  []gax.CallOption
	GetCryptoKeyVersion           []gax.CallOption
	CreateKeyRing                 []gax.CallOption
	CreateCryptoKey               []gax.CallOption
	CreateCryptoKeyVersion        []gax.CallOption
	UpdateCryptoKey               []gax.CallOption
	UpdateCryptoKeyVersion        []gax.CallOption
	Encrypt                       []gax.CallOption
	Decrypt                       []gax.CallOption
	UpdateCryptoKeyPrimaryVersion []gax.CallOption
	DestroyCryptoKeyVersion       []gax.CallOption
	RestoreCryptoKeyVersion       []gax.CallOption
}

func defaultKeyManagementClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("cloudkms.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultKeyManagementCallOptions() *KeyManagementCallOptions {
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
	return &KeyManagementCallOptions{
		ListKeyRings:           retry[[2]string{"default", "idempotent"}],
		ListCryptoKeys:         retry[[2]string{"default", "idempotent"}],
		ListCryptoKeyVersions:  retry[[2]string{"default", "idempotent"}],
		GetKeyRing:             retry[[2]string{"default", "idempotent"}],
		GetCryptoKey:           retry[[2]string{"default", "idempotent"}],
		GetCryptoKeyVersion:    retry[[2]string{"default", "idempotent"}],
		CreateKeyRing:          retry[[2]string{"default", "non_idempotent"}],
		CreateCryptoKey:        retry[[2]string{"default", "non_idempotent"}],
		CreateCryptoKeyVersion: retry[[2]string{"default", "non_idempotent"}],
		UpdateCryptoKey:        retry[[2]string{"default", "non_idempotent"}],
		UpdateCryptoKeyVersion: retry[[2]string{"default", "non_idempotent"}],
		Encrypt:                retry[[2]string{"default", "non_idempotent"}],
		Decrypt:                retry[[2]string{"default", "non_idempotent"}],
		UpdateCryptoKeyPrimaryVersion: retry[[2]string{"default", "non_idempotent"}],
		DestroyCryptoKeyVersion:       retry[[2]string{"default", "non_idempotent"}],
		RestoreCryptoKeyVersion:       retry[[2]string{"default", "non_idempotent"}],
	}
}

// KeyManagementClient is a client for interacting with Google Cloud Key Management Service (KMS) API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type KeyManagementClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	keyManagementClient kmspb.KeyManagementServiceClient

	// The call options for this service.
	CallOptions *KeyManagementCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewKeyManagementClient creates a new key management service client.
//
// Google Cloud Key Management Service
//
// Manages cryptographic keys and operations using those keys. Implements a REST
// model with the following objects:
//
//   [KeyRing][google.cloud.kms.v1.KeyRing]
//
//   [CryptoKey][google.cloud.kms.v1.CryptoKey]
//
//   [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion]
func NewKeyManagementClient(ctx context.Context, opts ...option.ClientOption) (*KeyManagementClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultKeyManagementClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &KeyManagementClient{
		conn:        conn,
		CallOptions: defaultKeyManagementCallOptions(),

		keyManagementClient: kmspb.NewKeyManagementServiceClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *KeyManagementClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *KeyManagementClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *KeyManagementClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListKeyRings lists [KeyRings][google.cloud.kms.v1.KeyRing].
func (c *KeyManagementClient) ListKeyRings(ctx context.Context, req *kmspb.ListKeyRingsRequest, opts ...gax.CallOption) *KeyRingIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListKeyRings[0:len(c.CallOptions.ListKeyRings):len(c.CallOptions.ListKeyRings)], opts...)
	it := &KeyRingIterator{}
	req = proto.Clone(req).(*kmspb.ListKeyRingsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*kmspb.KeyRing, string, error) {
		var resp *kmspb.ListKeyRingsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.keyManagementClient.ListKeyRings(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.KeyRings, resp.NextPageToken, nil
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

// ListCryptoKeys lists [CryptoKeys][google.cloud.kms.v1.CryptoKey].
func (c *KeyManagementClient) ListCryptoKeys(ctx context.Context, req *kmspb.ListCryptoKeysRequest, opts ...gax.CallOption) *CryptoKeyIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListCryptoKeys[0:len(c.CallOptions.ListCryptoKeys):len(c.CallOptions.ListCryptoKeys)], opts...)
	it := &CryptoKeyIterator{}
	req = proto.Clone(req).(*kmspb.ListCryptoKeysRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*kmspb.CryptoKey, string, error) {
		var resp *kmspb.ListCryptoKeysResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.keyManagementClient.ListCryptoKeys(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.CryptoKeys, resp.NextPageToken, nil
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

// ListCryptoKeyVersions lists [CryptoKeyVersions][google.cloud.kms.v1.CryptoKeyVersion].
func (c *KeyManagementClient) ListCryptoKeyVersions(ctx context.Context, req *kmspb.ListCryptoKeyVersionsRequest, opts ...gax.CallOption) *CryptoKeyVersionIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListCryptoKeyVersions[0:len(c.CallOptions.ListCryptoKeyVersions):len(c.CallOptions.ListCryptoKeyVersions)], opts...)
	it := &CryptoKeyVersionIterator{}
	req = proto.Clone(req).(*kmspb.ListCryptoKeyVersionsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*kmspb.CryptoKeyVersion, string, error) {
		var resp *kmspb.ListCryptoKeyVersionsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.keyManagementClient.ListCryptoKeyVersions(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.CryptoKeyVersions, resp.NextPageToken, nil
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

// GetKeyRing returns metadata for a given [KeyRing][google.cloud.kms.v1.KeyRing].
func (c *KeyManagementClient) GetKeyRing(ctx context.Context, req *kmspb.GetKeyRingRequest, opts ...gax.CallOption) (*kmspb.KeyRing, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetKeyRing[0:len(c.CallOptions.GetKeyRing):len(c.CallOptions.GetKeyRing)], opts...)
	var resp *kmspb.KeyRing
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.GetKeyRing(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetCryptoKey returns metadata for a given [CryptoKey][google.cloud.kms.v1.CryptoKey], as well as its
// [primary][google.cloud.kms.v1.CryptoKey.primary] [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion].
func (c *KeyManagementClient) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest, opts ...gax.CallOption) (*kmspb.CryptoKey, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetCryptoKey[0:len(c.CallOptions.GetCryptoKey):len(c.CallOptions.GetCryptoKey)], opts...)
	var resp *kmspb.CryptoKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.GetCryptoKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetCryptoKeyVersion returns metadata for a given [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion].
func (c *KeyManagementClient) GetCryptoKeyVersion(ctx context.Context, req *kmspb.GetCryptoKeyVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKeyVersion, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetCryptoKeyVersion[0:len(c.CallOptions.GetCryptoKeyVersion):len(c.CallOptions.GetCryptoKeyVersion)], opts...)
	var resp *kmspb.CryptoKeyVersion
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.GetCryptoKeyVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateKeyRing create a new [KeyRing][google.cloud.kms.v1.KeyRing] in a given Project and Location.
func (c *KeyManagementClient) CreateKeyRing(ctx context.Context, req *kmspb.CreateKeyRingRequest, opts ...gax.CallOption) (*kmspb.KeyRing, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateKeyRing[0:len(c.CallOptions.CreateKeyRing):len(c.CallOptions.CreateKeyRing)], opts...)
	var resp *kmspb.KeyRing
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.CreateKeyRing(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateCryptoKey create a new [CryptoKey][google.cloud.kms.v1.CryptoKey] within a [KeyRing][google.cloud.kms.v1.KeyRing].
//
// [CryptoKey.purpose][google.cloud.kms.v1.CryptoKey.purpose] is required.
func (c *KeyManagementClient) CreateCryptoKey(ctx context.Context, req *kmspb.CreateCryptoKeyRequest, opts ...gax.CallOption) (*kmspb.CryptoKey, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateCryptoKey[0:len(c.CallOptions.CreateCryptoKey):len(c.CallOptions.CreateCryptoKey)], opts...)
	var resp *kmspb.CryptoKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.CreateCryptoKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateCryptoKeyVersion create a new [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion] in a [CryptoKey][google.cloud.kms.v1.CryptoKey].
//
// The server will assign the next sequential id. If unset,
// [state][google.cloud.kms.v1.CryptoKeyVersion.state] will be set to
// [ENABLED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.ENABLED].
func (c *KeyManagementClient) CreateCryptoKeyVersion(ctx context.Context, req *kmspb.CreateCryptoKeyVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKeyVersion, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", req.GetParent()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateCryptoKeyVersion[0:len(c.CallOptions.CreateCryptoKeyVersion):len(c.CallOptions.CreateCryptoKeyVersion)], opts...)
	var resp *kmspb.CryptoKeyVersion
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.CreateCryptoKeyVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateCryptoKey update a [CryptoKey][google.cloud.kms.v1.CryptoKey].
func (c *KeyManagementClient) UpdateCryptoKey(ctx context.Context, req *kmspb.UpdateCryptoKeyRequest, opts ...gax.CallOption) (*kmspb.CryptoKey, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "crypto_key.name", req.GetCryptoKey().GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateCryptoKey[0:len(c.CallOptions.UpdateCryptoKey):len(c.CallOptions.UpdateCryptoKey)], opts...)
	var resp *kmspb.CryptoKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.UpdateCryptoKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateCryptoKeyVersion update a [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion]'s metadata.
//
// [state][google.cloud.kms.v1.CryptoKeyVersion.state] may be changed between
// [ENABLED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.ENABLED] and
// [DISABLED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DISABLED] using this
// method. See [DestroyCryptoKeyVersion][google.cloud.kms.v1.KeyManagementService.DestroyCryptoKeyVersion] and [RestoreCryptoKeyVersion][google.cloud.kms.v1.KeyManagementService.RestoreCryptoKeyVersion] to
// move between other states.
func (c *KeyManagementClient) UpdateCryptoKeyVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKeyVersion, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "crypto_key_version.name", req.GetCryptoKeyVersion().GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateCryptoKeyVersion[0:len(c.CallOptions.UpdateCryptoKeyVersion):len(c.CallOptions.UpdateCryptoKeyVersion)], opts...)
	var resp *kmspb.CryptoKeyVersion
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.UpdateCryptoKeyVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Encrypt encrypts data, so that it can only be recovered by a call to [Decrypt][google.cloud.kms.v1.KeyManagementService.Decrypt].
func (c *KeyManagementClient) Encrypt(ctx context.Context, req *kmspb.EncryptRequest, opts ...gax.CallOption) (*kmspb.EncryptResponse, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.Encrypt[0:len(c.CallOptions.Encrypt):len(c.CallOptions.Encrypt)], opts...)
	var resp *kmspb.EncryptResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.Encrypt(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Decrypt decrypts data that was protected by [Encrypt][google.cloud.kms.v1.KeyManagementService.Encrypt].
func (c *KeyManagementClient) Decrypt(ctx context.Context, req *kmspb.DecryptRequest, opts ...gax.CallOption) (*kmspb.DecryptResponse, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.Decrypt[0:len(c.CallOptions.Decrypt):len(c.CallOptions.Decrypt)], opts...)
	var resp *kmspb.DecryptResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.Decrypt(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateCryptoKeyPrimaryVersion update the version of a [CryptoKey][google.cloud.kms.v1.CryptoKey] that will be used in [Encrypt][google.cloud.kms.v1.KeyManagementService.Encrypt]
func (c *KeyManagementClient) UpdateCryptoKeyPrimaryVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyPrimaryVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKey, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateCryptoKeyPrimaryVersion[0:len(c.CallOptions.UpdateCryptoKeyPrimaryVersion):len(c.CallOptions.UpdateCryptoKeyPrimaryVersion)], opts...)
	var resp *kmspb.CryptoKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.UpdateCryptoKeyPrimaryVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DestroyCryptoKeyVersion schedule a [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion] for destruction.
//
// Upon calling this method, [CryptoKeyVersion.state][google.cloud.kms.v1.CryptoKeyVersion.state] will be set to
// [DESTROY_SCHEDULED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED]
// and [destroy_time][google.cloud.kms.v1.CryptoKeyVersion.destroy_time] will be set to a time 24
// hours in the future, at which point the [state][google.cloud.kms.v1.CryptoKeyVersion.state]
// will be changed to
// [DESTROYED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROYED], and the key
// material will be irrevocably destroyed.
//
// Before the [destroy_time][google.cloud.kms.v1.CryptoKeyVersion.destroy_time] is reached,
// [RestoreCryptoKeyVersion][google.cloud.kms.v1.KeyManagementService.RestoreCryptoKeyVersion] may be called to reverse the process.
func (c *KeyManagementClient) DestroyCryptoKeyVersion(ctx context.Context, req *kmspb.DestroyCryptoKeyVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKeyVersion, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DestroyCryptoKeyVersion[0:len(c.CallOptions.DestroyCryptoKeyVersion):len(c.CallOptions.DestroyCryptoKeyVersion)], opts...)
	var resp *kmspb.CryptoKeyVersion
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.DestroyCryptoKeyVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RestoreCryptoKeyVersion restore a [CryptoKeyVersion][google.cloud.kms.v1.CryptoKeyVersion] in the
// [DESTROY_SCHEDULED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED],
// state.
//
// Upon restoration of the CryptoKeyVersion, [state][google.cloud.kms.v1.CryptoKeyVersion.state]
// will be set to [DISABLED][google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DISABLED],
// and [destroy_time][google.cloud.kms.v1.CryptoKeyVersion.destroy_time] will be cleared.
func (c *KeyManagementClient) RestoreCryptoKeyVersion(ctx context.Context, req *kmspb.RestoreCryptoKeyVersionRequest, opts ...gax.CallOption) (*kmspb.CryptoKeyVersion, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", req.GetName()))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.RestoreCryptoKeyVersion[0:len(c.CallOptions.RestoreCryptoKeyVersion):len(c.CallOptions.RestoreCryptoKeyVersion)], opts...)
	var resp *kmspb.CryptoKeyVersion
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.keyManagementClient.RestoreCryptoKeyVersion(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CryptoKeyIterator manages a stream of *kmspb.CryptoKey.
type CryptoKeyIterator struct {
	items    []*kmspb.CryptoKey
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*kmspb.CryptoKey, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *CryptoKeyIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *CryptoKeyIterator) Next() (*kmspb.CryptoKey, error) {
	var item *kmspb.CryptoKey
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *CryptoKeyIterator) bufLen() int {
	return len(it.items)
}

func (it *CryptoKeyIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// CryptoKeyVersionIterator manages a stream of *kmspb.CryptoKeyVersion.
type CryptoKeyVersionIterator struct {
	items    []*kmspb.CryptoKeyVersion
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*kmspb.CryptoKeyVersion, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *CryptoKeyVersionIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *CryptoKeyVersionIterator) Next() (*kmspb.CryptoKeyVersion, error) {
	var item *kmspb.CryptoKeyVersion
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *CryptoKeyVersionIterator) bufLen() int {
	return len(it.items)
}

func (it *CryptoKeyVersionIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// KeyRingIterator manages a stream of *kmspb.KeyRing.
type KeyRingIterator struct {
	items    []*kmspb.KeyRing
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*kmspb.KeyRing, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *KeyRingIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *KeyRingIterator) Next() (*kmspb.KeyRing, error) {
	var item *kmspb.KeyRing
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *KeyRingIterator) bufLen() int {
	return len(it.items)
}

func (it *KeyRingIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
