// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
)

func TestPushPermissionHint(t *testing.T) {
	denied := status.Error(codes.PermissionDenied, "write denied")
	permissionDeniedRPC := remotestorage.NewRpcError(denied, "Commit", "test-host", nil)
	unavailableRPC := remotestorage.NewRpcError(status.Error(codes.Unavailable, "offline"), "Commit", "test-host", nil)
	for _, tt := range []struct {
		name             string
		err              error
		expectLoginHint  bool
		expectRPCDetails bool
	}{
		{"success", nil, false, false},
		{"unrelated error", errors.New("other failure"), false, false},
		{"unknown push", actions.ErrUnknownPushErr, false, false},
		{"wrapped permission denied", fmt.Errorf("%w; %w", actions.ErrUnknownPushErr, denied), true, false},
		{"wrapped RPC permission denied", fmt.Errorf("%w; %w", actions.ErrUnknownPushErr, permissionDeniedRPC), true, true},
		{"unavailable", fmt.Errorf("%w; %w", actions.ErrUnknownPushErr, status.Error(codes.Unavailable, "offline")), false, false},
		{"wrapped RPC unavailable", fmt.Errorf("%w; %w", actions.ErrUnknownPushErr, unavailableRPC), false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			oldOut, oldErr := cli.CliOut, cli.CliErr
			cli.CliOut, cli.CliErr = &out, &errOut
			defer func() { cli.CliOut, cli.CliErr = oldOut, oldErr }()
			code := handlePushError(tt.err, func() {})
			if tt.err == nil {
				require.Zero(t, code)
			} else {
				require.Equal(t, 1, code)
			}
			if tt.expectLoginHint {
				require.Contains(t, out.String(), "dolt login")
				require.Contains(t, out.String(), "has write perms to DoltHub repo")
			} else {
				require.NotContains(t, out.String(), "dolt login")
				require.NotContains(t, out.String(), "has write perms to DoltHub repo")
			}
			if tt.expectRPCDetails {
				require.Contains(t, errOut.String(), "host:test-host")
				require.Contains(t, errOut.String(), "rpc: Commit")
			}
		})
	}
}
