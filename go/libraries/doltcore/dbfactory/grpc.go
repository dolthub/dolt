package dbfactory

import (
	"context"
	"fmt"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"google.golang.org/grpc"
	"net/url"
)

// GRPCConnectionProvider is an interface for getting a *grpc.ClientConn.
type GRPCConnectionProvider interface {
	GrpcConn(hostAndPort string, insecure bool) (*grpc.ClientConn, error)
}

// DoldRemoteFactory is a DBFactory implementation for creating databases backed by a remote server that implements the
// GRPC rpcs defined by remoteapis.ChunkStoreServiceClient
type DoltRemoteFactory struct {
	grpcCP   GRPCConnectionProvider
	insecure bool
}

// NewDoltRemoteFactory creates a DoltRemoteFactory instance using the given GRPCConnectionProvider, and insecure setting
func NewDoltRemoteFactory(grpcCP GRPCConnectionProvider, insecure bool) DoltRemoteFactory {
	return DoltRemoteFactory{grpcCP, insecure}
}

// CreateDB creates a database backed by a remote server that implements the GRPC rpcs defined by
// remoteapis.ChunkStoreServiceClient
func (fact DoltRemoteFactory) CreateDB(ctx context.Context, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database

	cs, err := fact.newChunkStore(ctx, urlObj, params)

	if err != nil {
		return nil, err
	}

	db = datas.NewDatabase(cs)

	return db, err
}

func (fact DoltRemoteFactory) newChunkStore(ctx context.Context, urlObj *url.URL, params map[string]string) (chunks.ChunkStore, error) {
	conn, err := fact.grpcCP.GrpcConn(urlObj.Host, fact.insecure)

	if err != nil {
		return nil, err
	}

	csClient := remotesapi.NewChunkStoreServiceClient(conn)
	cs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, urlObj.Path, urlObj.Host, csClient)

	if err == remotestorage.ErrInvalidDoltSpecPath {
		return nil, fmt.Errorf("invalid dolt url '%s'", urlObj.String())
	}

	return cs, err
}
