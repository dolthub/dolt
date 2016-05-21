package flags

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/hash"
	"github.com/attic-labs/noms/types"
)

var (
	storeRegex = regexp.MustCompile("^([^:]+):?(.+)?$")
	pathRegex  = regexp.MustCompile("^(.+):(.+)$")
)

type DatabaseSpec struct {
	Protocol    string
	Path        string
	accessToken string
}

type DatasetSpec struct {
	StoreSpec   DatabaseSpec
	DatasetName string
}

type RefSpec struct {
	StoreSpec DatabaseSpec
	Ref       hash.Hash
}

type PathSpec interface {
	Value() (datas.Database, types.Value, error)
}

func ParseDatabaseSpec(spec string) (DatabaseSpec, error) {
	parts := storeRegex.FindStringSubmatch(spec)
	if len(parts) != 3 {
		return DatabaseSpec{}, fmt.Errorf("Invalid database spec: %s", spec)
	}
	protocol := parts[1]
	path := parts[2]
	switch protocol {
	case "http", "https":
		if len(parts[2]) == 0 {
			return DatabaseSpec{}, fmt.Errorf("Invalid database spec: %s", spec)
		}
		u, err := url.Parse(spec)
		if err != nil {
			return DatabaseSpec{}, fmt.Errorf("Invalid path for %s protocol, spec: %s\n", protocol, spec)
		}
		token := u.Query().Get("access_token")
		return DatabaseSpec{Protocol: protocol, Path: path, accessToken: token}, nil
	case "ldb":
		if len(parts[2]) == 0 {
			return DatabaseSpec{}, fmt.Errorf("Invalid database spec: %s", spec)
		}
		return DatabaseSpec{Protocol: protocol, Path: path}, nil
	case "mem":
		if len(parts[2]) > 0 {
			return DatabaseSpec{}, fmt.Errorf("Invalid database spec: %s", spec)
		}
		return DatabaseSpec{Protocol: protocol, Path: ""}, nil
	}
	return DatabaseSpec{}, fmt.Errorf("Invalid database spec: %s", spec)
}

func ParseDatasetSpec(spec string) (DatasetSpec, error) {
	parts := pathRegex.FindStringSubmatch(spec)
	if len(parts) != 3 {
		return DatasetSpec{}, fmt.Errorf("Invalid dataset spec: %s", spec)
	}
	storeSpec, err := ParseDatabaseSpec(parts[1])
	if err != nil {
		return DatasetSpec{}, err
	}
	return DatasetSpec{StoreSpec: storeSpec, DatasetName: parts[2]}, nil
}

func ParseRefSpec(spec string) (RefSpec, error) {
	dspec, err := ParseDatasetSpec(spec)
	if err != nil {
		return RefSpec{}, err
	}

	if r, ok := hash.MaybeParse(dspec.DatasetName); ok {
		return RefSpec{StoreSpec: dspec.StoreSpec, Ref: r}, nil
	}

	return RefSpec{}, fmt.Errorf("Invalid path spec: %s", spec)
}

func ParsePathSpec(spec string) (PathSpec, error) {
	var pathSpec PathSpec
	if rspec, err := ParseRefSpec(spec); err == nil {
		pathSpec = &rspec
	} else if dspec, err := ParseDatasetSpec(spec); err == nil {
		pathSpec = &dspec
	} else {
		return nil, fmt.Errorf("Invalid path spec: %s", spec)
	}
	return pathSpec, nil

}

func (s DatabaseSpec) String() string {
	return s.Protocol + ":" + s.Path
}

func (spec DatabaseSpec) Database() (ds datas.Database, err error) {
	switch spec.Protocol {
	case "http", "https":
		ds = datas.NewRemoteDatabase(spec.String(), "Bearer "+spec.accessToken)
	case "ldb":
		ds = datas.NewDatabase(chunks.NewLevelDBStoreUseFlags(spec.Path, ""))
	case "mem":
		ds = datas.NewDatabase(chunks.NewMemoryStore())
	default:
		err = fmt.Errorf("Invalid path prototocol: %s", spec.Protocol)
	}
	return
}

func (spec DatabaseSpec) ChunkStore() (cs chunks.ChunkStore, err error) {
	switch spec.Protocol {
	case "ldb":
		cs = chunks.NewLevelDBStoreUseFlags(spec.Path, "")
	case "mem":
		cs = chunks.NewMemoryStore()
	default:
		return nil, fmt.Errorf("Unable to create chunkstore for protocol: %s", spec)
	}

	return
}

func (spec DatasetSpec) Dataset() (dataset.Dataset, error) {
	store, err := spec.StoreSpec.Database()
	if err != nil {
		return dataset.Dataset{}, err
	}

	return dataset.NewDataset(store, spec.DatasetName), nil
}

func (spec DatasetSpec) Value() (datas.Database, types.Value, error) {
	dataset, err := spec.Dataset()
	if err != nil {
		return nil, nil, err
	}

	commit, ok := dataset.MaybeHead()
	if !ok {
		return nil, nil, fmt.Errorf("No head value for dataset: %s", spec.DatasetName)
	}

	return dataset.Store(), commit, nil
}

func (spec RefSpec) Value() (datas.Database, types.Value, error) {
	store, err := spec.StoreSpec.Database()
	if err != nil {
		return nil, nil, err
	}
	return store, store.ReadValue(spec.Ref), nil
}

func RegisterDatabaseFlags() {
	chunks.RegisterLevelDBFlags()
}
