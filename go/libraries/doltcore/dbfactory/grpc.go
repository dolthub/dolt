package dbfactory

import (
	"context"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
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
	err := pantoerr.PanicToError("failed to create database", func() error {
		cs, err := fact.newChunkStore(urlObj, params)

		if err != nil {
			return err
		}

		db = datas.NewDatabase(cs)

		return nil
	})

	return db, err
}

func (fact DoltRemoteFactory) newChunkStore(urlObj *url.URL, params map[string]string) (chunks.ChunkStore, error) {
	conn, err := fact.grpcCP.GrpcConn(urlObj.Host, fact.insecure)

	if err != nil {
		return nil, err
	}

	csClient := remotesapi.NewChunkStoreServiceClient(conn)
	return remotestorage.NewDoltChunkStoreFromPath(urlObj.Path, urlObj.Host, csClient)
}
