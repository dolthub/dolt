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

package kms_test

import (
	"cloud.google.com/go/kms/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
)

func ExampleNewKeyManagementClient() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleKeyManagementClient_ListKeyRings() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.ListKeyRingsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListKeyRings(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleKeyManagementClient_ListCryptoKeys() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.ListCryptoKeysRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListCryptoKeys(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleKeyManagementClient_ListCryptoKeyVersions() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.ListCryptoKeyVersionsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListCryptoKeyVersions(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleKeyManagementClient_GetKeyRing() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.GetKeyRingRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetKeyRing(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_GetCryptoKey() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.GetCryptoKeyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetCryptoKey(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_GetCryptoKeyVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.GetCryptoKeyVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetCryptoKeyVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_CreateKeyRing() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.CreateKeyRingRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateKeyRing(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_CreateCryptoKey() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.CreateCryptoKeyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateCryptoKey(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_CreateCryptoKeyVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.CreateCryptoKeyVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateCryptoKeyVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_UpdateCryptoKey() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.UpdateCryptoKeyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateCryptoKey(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_UpdateCryptoKeyVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.UpdateCryptoKeyVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateCryptoKeyVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_Encrypt() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.EncryptRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Encrypt(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_Decrypt() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.DecryptRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Decrypt(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_UpdateCryptoKeyPrimaryVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateCryptoKeyPrimaryVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_DestroyCryptoKeyVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.DestroyCryptoKeyVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.DestroyCryptoKeyVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleKeyManagementClient_RestoreCryptoKeyVersion() {
	ctx := context.Background()
	c, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &kmspb.RestoreCryptoKeyVersionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.RestoreCryptoKeyVersion(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
