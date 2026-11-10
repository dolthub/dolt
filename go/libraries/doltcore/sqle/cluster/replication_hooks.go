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

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/types"
)

// openStandbyDB opens |remote|, the standby named |remoteName| that |c| replicates |ddb| to.
var openStandbyDB = func(c *Controller, ctx context.Context, remoteName string, remote env.Remote, nbf *types.NomsBinFormat, ddb *doltdb.DoltDB, dialprovider dbfactory.GRPCDialProvider) (*doltdb.DoltDB, error) {
	return remote.GetRemoteDBWithoutCaching(ctx, nbf, dialprovider)
}

// registerGrpcServices registers the replication services a server offers beyond Dolt's own on |srv|.
var registerGrpcServices = func(srv *grpc.Server) {}

// newReplicationServiceClient returns the client of the replication services of a standby that |cc| reaches.
var newReplicationServiceClient = func(cc grpc.ClientConnInterface) replicationapi.ReplicationServiceClient {
	return replicationapi.NewReplicationServiceClient(cc)
}
