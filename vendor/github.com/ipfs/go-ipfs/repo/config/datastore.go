package config

import (
	"encoding/json"
)

// DefaultDataStoreDirectory is the directory to store all the local IPFS data.
const DefaultDataStoreDirectory = "datastore"

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	StorageMax         string // in B, kB, kiB, MB, ...
	StorageGCWatermark int64  // in percentage to multiply on StorageMax
	GCPeriod           string // in ns, us, ms, s, m, h

	// deprecated fields, use Spec
	Type   string           `json:",omitempty"`
	Path   string           `json:",omitempty"`
	NoSync bool             `json:",omitempty"`
	Params *json.RawMessage `json:",omitempty"`

	Spec map[string]interface{}

	HashOnRead      bool
	BloomFilterSize int
}

// DataStorePath returns the default data store path given a configuration root
// (set an empty string to have the default configuration root)
func DataStorePath(configroot string) (string, error) {
	return Path(configroot, DefaultDataStoreDirectory)
}
