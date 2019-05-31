package env

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/earl"
	"regexp"
)

const (
	DoltNomsProtocolID = "dolt"
)

var hasSchemaRegEx = regexp.MustCompile("^[a-zA-Z][-+.a-zA-Z0-9]*://")

type DoltProtocol struct {
	dEnv *DoltEnv
}

func (dhp DoltProtocol) NewChunkStore(sp spec.Spec) (chunks.ChunkStore, error) {
	remotes, err := dhp.dEnv.GetRemotes()

	if err != nil {
		return nil, err
	}

	r := remotes[sp.DatabaseName]
	remoteUrl, err := earl.Parse(r.Url)

	if err != nil {
		return nil, err
	}

	conn, err := dhp.dEnv.GrpcConn(remoteUrl.Host, IsInsecure(r))

	if err != nil {
		return nil, err
	}

	csClient := remotesapi.NewChunkStoreServiceClient(conn)
	return remotestorage.NewDoltChunkStoreFromPath(remoteUrl.Path, remoteUrl.Host, csClient)
}

func (dhp DoltProtocol) NewDatabase(sp spec.Spec) (datas.Database, error) {
	cs, err := dhp.NewChunkStore(sp)

	if err != nil {
		return nil, err
	}

	return datas.NewDatabase(cs), nil
}
