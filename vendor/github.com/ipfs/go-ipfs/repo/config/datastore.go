package config

import (
	"encoding/json"
)

// DefaultDataStoreDirectory is the directory to store all the local IPFS data.
const DefaultDataStoreDirectory = "datastore"

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	Type               string
	Path               string
	StorageMax         string // in B, kB, kiB, MB, ...
	StorageGCWatermark int64  // in percentage to multiply on StorageMax
	GCPeriod           string // in ns, us, ms, s, m, h

	Params          *json.RawMessage
	NoSync          bool
	HashOnRead      bool
	BloomFilterSize int
}

func (d *Datastore) ParamData() []byte {
	if d.Params == nil {
		return nil
	}

	return []byte(*d.Params)
}

type S3Datastore struct {
	Region string `json:"region"`
	Bucket string `json:"bucket"`
	ACL    string `json:"acl"`
}

// DataStorePath returns the default data store path given a configuration root
// (set an empty string to have the default configuration root)
func DataStorePath(configroot string) (string, error) {
	return Path(configroot, DefaultDataStoreDirectory)
}
