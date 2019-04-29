package env

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
)

type Remote struct {
	Name     string `json:"name"`
	Url      string `json:"url"`
	Insecure *bool  `json:"insecure"`
}

func NewRemote(name, url string, insecure bool) Remote {
	return Remote{name, url, &insecure}
}

func IsInsecure(r Remote) bool {
	return r.Insecure != nil && *r.Insecure
}

func (r *Remote) GetRemoteDB(ctx context.Context) *doltdb.DoltDB {
	remoteLocStr := DoltNomsProtocolID + ":" + r.Name
	return doltdb.LoadDoltDB(ctx, doltdb.Location(remoteLocStr))
}
