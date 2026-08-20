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

package commands

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
)

func TestIsPermissionDeniedErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "wrapped RpcError with PermissionDenied",
			err:      wrappedRpcError(codes.PermissionDenied, "access denied"),
			expected: true,
		},
		{
			name:     "wrapped RpcError with Internal",
			err:      wrappedRpcError(codes.Internal, "server error"),
			expected: false,
		},
		{
			name:     "wrapped RpcError with Unauthenticated",
			err:      wrappedRpcError(codes.Unauthenticated, "not authenticated"),
			expected: false,
		},
		{
			name:     "plain error with PermissionDenied in text",
			err:      fmt.Errorf("%w: rpc error: code = PermissionDenied desc = no access", actions.ErrUnknownPushErr),
			expected: true,
		},
		{
			name:     "plain error without PermissionDenied",
			err:      fmt.Errorf("%w: connection refused", actions.ErrUnknownPushErr),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				assert.False(t, isPermissionDeniedErr(errors.New("")))
				return
			}
			assert.Equal(t, tt.expected, isPermissionDeniedErr(tt.err))
		})
	}
}

func wrappedRpcError(code codes.Code, msg string) error {
	grpcErr := status.Error(code, msg)
	rpcErr := remotestorage.NewRpcError(grpcErr, "Push", "dolthub.com", nil)
	return fmt.Errorf("%w: %w", actions.ErrUnknownPushErr, rpcErr)
}
