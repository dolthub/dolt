package flags

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
)

const (
	maxFileHandles = 24
)

var (
	validDatasetNameRegexp = regexp.MustCompile("^[a-zA-Z0-9]+([/\\-_][a-zA-Z0-9]+)*$")
)

func ParseDataStore(in string) (ds datas.DataStore, err error) {
	input := strings.Split(in, ":")

	if len(input) < 2 {
		return ds, fmt.Errorf("Improper datastore name: %s", in)
	}

	switch input[0] {
	case "http":
		//get from server and path, including http
		ds = datas.NewRemoteDataStore(in, "")

	case "ldb":
		//create/access from path
		ds = datas.NewDataStore(chunks.NewLevelDBStore(strings.Join(input[1:len(input)], ":"), "", maxFileHandles, false))

	case "mem":
		ds = datas.NewDataStore(chunks.NewMemoryStore())

	case "":
		ds = datas.NewDataStore(chunks.NewLevelDBStore("$HOME/.noms", "", maxFileHandles, false))

	default:
		err = fmt.Errorf("Improper datastore name: %s", in)

	}

	return
}

func ParseDataset(in string) (dataset.Dataset, error) {
	input := strings.Split(in, ":")

	if len(input) < 3 {
		return dataset.Dataset{}, fmt.Errorf("Improper dataset name: %s", in)
	}

	ds, errStore := ParseDataStore(strings.Join(input[:len(input)-1], ":"))
	name := input[len(input)-1]

	d.Chk.NoError(errStore)

	if !validDatasetNameRegexp.MatchString(name) {
		return dataset.Dataset{}, fmt.Errorf("Improper dataset name: %s", in)
	}

	return dataset.NewDataset(ds, name), nil
}

func ParseObject(in string) (dataset.Dataset, ref.Ref, bool, error) {
	input := strings.Split(in, ":")

	if len(input) < 3 {
		return dataset.Dataset{}, ref.Ref{}, false, fmt.Errorf("Improper object name: %s", in)
	}

	objectName := input[len(input)-1]

	if r, isRef := ref.MaybeParse(objectName); isRef {
		return dataset.Dataset{}, r, false, nil
	}

	ds, isValid := ParseDataset(in)

	d.Chk.NoError(isValid)

	return ds, ref.Ref{}, true, nil
}
