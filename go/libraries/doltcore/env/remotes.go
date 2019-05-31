package env

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
)

var NoRemote = Remote{}

type Remote struct {
	Name       string   `json:"name"`
	Url        string   `json:"url"`
	Insecure   *bool    `json:"insecure"`
	FetchSpecs []string `json:"fetch_specs"`
}

func NewRemote(name, url string, insecure bool) Remote {
	return Remote{name, url, &insecure, []string{"refs/heads/*:refs/remotes/" + name + "/*"}}
}

func IsInsecure(r Remote) bool {
	return r.Insecure != nil && *r.Insecure
}

func (r *Remote) GetRemoteDB(ctx context.Context) (*doltdb.DoltDB, error) {
	remoteLocStr := DoltNomsProtocolID + ":" + r.Name
	return doltdb.LoadDoltDB(ctx, doltdb.Location(remoteLocStr))
}
