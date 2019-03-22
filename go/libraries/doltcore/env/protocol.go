package env

import (
	"errors"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"regexp"
	"strings"
)

const (
	DoltNomsProtocolID = "dolt"
)

var hasSchemaRegEx = regexp.MustCompile("^[a-zA-Z][-+.a-zA-Z0-9]*://")

type DoltProtocol struct {
	dEnv *DoltEnv
}

func (dhp DoltProtocol) NewChunkStore(sp spec.Spec) (chunks.ChunkStore, error) {
	remoteUrl, err := ParseRemoteUrl(sp.DatabaseName)

	if err != nil {
		return nil, err
	}

	tokens := strings.Split(strings.Trim(remoteUrl.Path, "/"), "/")
	if len(tokens) != 2 {
		return nil, errors.New("dolt chunk store spec invalid")
	}

	org := tokens[0]
	repoName := tokens[1]

	conn, err := dhp.dEnv.GrpcConn(remoteUrl.Host)

	if err != nil {
		return nil, err
	}

	csClient := remotesapi.NewChunkStoreServiceClient(conn)
	return remotestorage.NewDoltChunkStore(org, repoName, remoteUrl.Host, csClient), nil
}

func (dhp DoltProtocol) NewDatabase(sp spec.Spec) (datas.Database, error) {
	cs, err := dhp.NewChunkStore(sp)

	if err != nil {
		return nil, err
	}

	return datas.NewDatabase(cs), nil
}
