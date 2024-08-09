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

package dbfactory

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

var GRPCDialProviderParam = "__DOLT__grpc_dial_provider"
var GRPCUsernameAuthParam = "__DOLT__grpc_username"

type GRPCRemoteConfig struct {
	Endpoint    string
	DialOptions []grpc.DialOption
	HTTPFetcher grpcendpoint.HTTPFetcher
}

// GRPCDialProvider is an interface for getting a concrete Endpoint,
// DialOptions and HTTPFetcher from a slightly more abstract
// grpcendpoint.Config. It allows a caller to override certain aspects of how
// the grpc.ClientConn and the resulting remotestorage ChunkStore are
// configured by dbfactory when it returns remotestorage DBs.
//
// An instance of this must be provided in |params[GRPCDialProviderParam]| when
// calling |CreateDB| with a remotesapi remote. See *env.Remote for example.
type GRPCDialProvider interface {
	GetGRPCDialParams(grpcendpoint.Config) (GRPCRemoteConfig, error)
}

// DoldRemoteFactory is a DBFactory implementation for creating databases backed by a remote server that implements the
// GRPC rpcs defined by remoteapis.ChunkStoreServiceClient
type DoltRemoteFactory struct {
	insecure bool
}

func (fact DoltRemoteFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	return fmt.Errorf("http(s) scheme cannot support this operation")
}

// NewDoltRemoteFactory creates a DoltRemoteFactory instance using the given GRPCConnectionProvider, and insecure setting
func NewDoltRemoteFactory(insecure bool) DoltRemoteFactory {
	return DoltRemoteFactory{insecure}
}

// CreateDB creates a database backed by a remote server that implements the GRPC rpcs defined by
// remoteapis.ChunkStoreServiceClient
func (fact DoltRemoteFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	var db datas.Database

	dpi, ok := params[GRPCDialProviderParam]
	if dpi == nil || !ok {
		return nil, nil, nil, errors.New("DoltRemoteFactory.CreateDB must provide a GRPCDialProvider param through GRPCDialProviderParam")
	}
	dp, ok := dpi.(GRPCDialProvider)
	if !ok {
		return nil, nil, nil, fmt.Errorf("DoltRemoteFactory.CreateDB must provide a GRPCDialProvider param through GRPCDialProviderParam: %v", dpi)
	}

	cs, err := fact.newChunkStore(ctx, nbf, urlObj, params, dp)

	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db = datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, err
}

// If |params[NoCachingParameter]| is set in |params| of the CreateDB call for
// a remotesapi database, then the configured database will have caching at the
// remotestorage.ChunkStore layer disabled.
var NoCachingParameter = "__dolt__NO_CACHING"

func (fact DoltRemoteFactory) newChunkStore(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}, dp GRPCDialProvider) (chunks.ChunkStore, error) {
	var user string
	wsValidate := false
	if userParam := params[GRPCUsernameAuthParam]; userParam != nil {
		user = userParam.(string)
		wsValidate = true
	}
	cfg, err := dp.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint:           urlObj.Host,
		Insecure:           fact.insecure,
		UserIdForOsEnvAuth: user,
		WithEnvCreds:       true,
	})
	if err != nil {
		return nil, err
	}

	opts := append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(remotestorage.EventsUnaryClientInterceptor(events.GlobalCollector())))
	opts = append(opts, grpc.WithChainUnaryInterceptor(remotestorage.RetryingUnaryClientInterceptor))

	conn, err := grpc.Dial(cfg.Endpoint, opts...)
	if err != nil {
		return nil, err
	}

	csClient := remotesapi.NewChunkStoreServiceClient(conn)
	cs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, nbf, urlObj.Path, urlObj.Host, wsValidate, csClient)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("could not access dolt url '%s': %w", urlObj.String(), err)
	}
	cs = cs.WithHTTPFetcher(cfg.HTTPFetcher)
	cs.SetFinalizer(conn.Close)

	if _, ok := params[NoCachingParameter]; ok {
		cs = cs.WithNoopChunkCache()
	}

	return cs, nil
}
