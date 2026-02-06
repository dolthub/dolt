// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitbs "github.com/dolthub/dolt/go/store/blobstore/internal/gitbs"
)

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (t *trackingReadCloser) Close() error {
	t.closed = true
	return nil
}

func TestMultiPartReadCloser_ReadConcatenatesAcrossPartsWithOffsets(t *testing.T) {
	ctx := context.Background()

	oid1 := "0123456789abcdef0123456789abcdef01234567"
	oid2 := "89abcdef0123456789abcdef0123456789abcdef"

	blobs := map[string][]byte{
		oid1: []byte("hello"),
		oid2: []byte("world!"),
	}

	api := fakeGitAPI{
		blobReader: func(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
			b, ok := blobs[oid.String()]
			require.True(t, ok, "unexpected oid %s", oid.String())
			return io.NopCloser(bytes.NewReader(b)), nil
		},
	}

	rc := &multiPartReadCloser{
		ctx: ctx,
		api: api,
		slices: []gitbs.PartSlice{
			{OIDHex: oid1, Offset: 1, Length: 3}, // "ell"
			{OIDHex: oid2, Offset: 2, Length: 3}, // "rld"
		},
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("ellrld"), got)
}

func TestMultiPartReadCloser_ReadUnexpectedEOFWhenPartShorterThanDeclared(t *testing.T) {
	ctx := context.Background()

	oid := "0123456789abcdef0123456789abcdef01234567"
	api := fakeGitAPI{
		blobReader: func(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("hi"))), nil // 2 bytes
		},
	}

	rc := &multiPartReadCloser{
		ctx: ctx,
		api: api,
		slices: []gitbs.PartSlice{
			{OIDHex: oid, Offset: 0, Length: 3}, // expect 3 bytes, only 2 available
		},
	}
	defer func() { _ = rc.Close() }()

	_, err := io.ReadAll(rc)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestMultiPartReadCloser_CloseClosesUnderlyingPartReader(t *testing.T) {
	ctx := context.Background()

	oid := "0123456789abcdef0123456789abcdef01234567"
	underlying := &trackingReadCloser{Reader: bytes.NewReader([]byte("hello"))}

	api := fakeGitAPI{
		blobReader: func(ctx context.Context, oid git.OID) (io.ReadCloser, error) {
			return underlying, nil
		},
	}

	rc := &multiPartReadCloser{
		ctx: ctx,
		api: api,
		slices: []gitbs.PartSlice{
			{OIDHex: oid, Offset: 0, Length: 1},
		},
	}

	// Force the underlying reader to be opened.
	buf := make([]byte, 1)
	_, err := rc.Read(buf)
	require.NoError(t, err)

	require.NoError(t, rc.Close())
	require.True(t, underlying.closed)
}
