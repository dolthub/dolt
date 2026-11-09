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

package cluster

import (
	"context"

	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/types"
)

// standbyCheck fails if standby |remote|, whose replication service |conn| reaches, cannot hold a replica of |ddb|.
// |conn| is nil if the standby has no replication client. Dolt Enterprise replaces it.
var standbyCheck = func(ctx context.Context, conn grpc.ClientConnInterface, remote string, ddb *doltdb.DoltDB) error {
	return nil
}

// grpcServices register services, beyond the replication service, that cluster members serve. Dolt Enterprise adds to it.
var grpcServices []func(*grpc.Server)

// openStandbyDB opens the database of standby |remoteName| that replicates |ddb|.
func (c *Controller) openStandbyDB(ctx context.Context, remoteName string, remote env.Remote, nbf *types.NomsBinFormat, ddb *doltdb.DoltDB, dialprovider dbfactory.GRPCDialProvider) (*doltdb.DoltDB, error) {
	if err := standbyCheck(ctx, c.standbyConn(remoteName), remoteName, ddb); err != nil {
		return nil, err
	}
	push, err := remote.ForPushFrom(ctx, ddb)
	if err != nil {
		return nil, err
	}
	return push.GetRemoteDBWithoutCaching(ctx, nbf, dialprovider)
}

func (c *Controller) standbyConn(remote string) grpc.ClientConnInterface {
	for _, client := range c.replicationClients {
		if client.remote == remote && client.conn != nil {
			return client.conn
		}
	}
	return nil
}
