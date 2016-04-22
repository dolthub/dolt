package flags

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	storeRegex = regexp.MustCompile("^(.+?)(:.+)?$")
	pathRegex  = regexp.MustCompile("^(.+):(.+)$")
)

type DatastoreSpec struct {
	Protocol string
	Path     string
}

type DatasetSpec struct {
	StoreSpec   DatastoreSpec
	DatasetName string
}

type RefSpec struct {
	StoreSpec DatastoreSpec
	Ref       ref.Ref
}

type PathSpec interface {
	Value() (datas.DataStore, types.Value, error)
}

func ParseDatastoreSpec(spec string) (DatastoreSpec, error) {
	res := storeRegex.FindStringSubmatch(spec)
	if len(res) != 3 {
		return DatastoreSpec{}, fmt.Errorf("Invalid datastore spec: %s", spec)
	}
	protocol := res[1]
	switch protocol {
	case "http", "https", "ldb":
		if len(res[2]) == 0 {
			return DatastoreSpec{}, fmt.Errorf("Invalid datastore spec: %s", spec)
		}
		return DatastoreSpec{Protocol: protocol, Path: strings.TrimRight(res[2][1:], "/")}, nil
	case "mem":
		if len(res[2]) > 0 {
			return DatastoreSpec{}, fmt.Errorf("Invalid datastore spec: %s", spec)
		}
		return DatastoreSpec{Protocol: protocol, Path: ""}, nil
	}
	return DatastoreSpec{}, fmt.Errorf("Invalid datastore spec: %s", spec)
}

func ParseDatasetSpec(spec string) (DatasetSpec, error) {
	res := pathRegex.FindStringSubmatch(spec)
	if len(res) != 3 {
		return DatasetSpec{}, fmt.Errorf("Invalid dataset spec: %s", spec)
	}
	storeSpec, err := ParseDatastoreSpec(res[1])
	if err != nil {
		return DatasetSpec{}, err
	}
	return DatasetSpec{StoreSpec: storeSpec, DatasetName: res[2]}, nil
}

func ParseRefSpec(spec string) (RefSpec, error) {
	dspec, err := ParseDatasetSpec(spec)
	if err != nil {
		return RefSpec{}, err
	}

	if r, ok := ref.MaybeParse(dspec.DatasetName); ok {
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

func (spec DatastoreSpec) Datastore() (ds datas.DataStore, err error) {
	switch spec.Protocol {
	case "http":
		ds = datas.NewRemoteDataStore(spec.Protocol+":"+spec.Path, "")
	case "ldb":
		ds = datas.NewDataStore(chunks.NewLevelDBStoreUseFlags(spec.Path, ""))
	case "mem":
		ds = datas.NewDataStore(chunks.NewMemoryStore())
	default:
		err = fmt.Errorf("Invalid path prototocol: %s", spec.Protocol)
	}
	return
}

func (spec DatastoreSpec) ChunkStore() (cs chunks.ChunkStore, err error) {
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
	store, err := spec.StoreSpec.Datastore()
	if err != nil {
		return dataset.Dataset{}, err
	}

	return dataset.NewDataset(store, spec.DatasetName), nil
}

func (spec DatasetSpec) Value() (datas.DataStore, types.Value, error) {
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

func (spec RefSpec) Value() (datas.DataStore, types.Value, error) {
	store, err := spec.StoreSpec.Datastore()
	if err != nil {
		return nil, nil, err
	}
	return store, store.ReadValue(spec.Ref), nil
}

func RegisterDataStoreFlags() {
	chunks.RegisterLevelDBFlags()
}
