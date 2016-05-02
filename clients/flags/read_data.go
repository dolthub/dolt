package flags

import (
	"fmt"
	"os"
	"path/filepath"
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

//ParseDatabase takes an optional colon-delineated string indicating what kind of Database to open and return. Supported syntax includes
// - http:<server and path>
// - ldb:<path>
// - mem:
func ParseDatabase(in string) (db datas.Database, err error) {
	input := strings.Split(in, ":")

	switch input[0] {
	case "http":
		//get from server and path, including http
		if len(input) < 2 {
			return db, fmt.Errorf("Improper database name: %s", in)
		}

		db = datas.NewRemoteDatabase(in, "")

	case "ldb":
		//create/access from path
		if len(input) < 2 {
			return db, fmt.Errorf("Improper database name: %s", in)
		}
		db = datas.NewDatabase(chunks.NewLevelDBStore(strings.Join(input[1:len(input)], ":"), "", maxFileHandles, false))

	case "mem":
		if len(input) < 2 {
			return db, fmt.Errorf("Improper database name: %s", in)
		}

		db = datas.NewDatabase(chunks.NewMemoryStore())

	case "":
		db = datas.NewDatabase(chunks.NewLevelDBStore(filepath.Join(os.Getenv("HOME"), ".noms"), "", maxFileHandles, false))

	default:
		err = fmt.Errorf("Improper database name: %s", in)

	}

	return
}

//ParseDataset takes a colon-delineated string indicating a Database and the name of a dataset to open and return. Supported syntax includes
//<database>:<dataset>
func ParseDataset(in string) (dataset.Dataset, error) {
	input := strings.Split(in, ":")

	if len(input) < 3 {
		return dataset.Dataset{}, fmt.Errorf("Improper dataset name: %s", in)
	}

	db, errStore := ParseDatabase(strings.Join(input[:len(input)-1], ":"))
	name := input[len(input)-1]

	d.Chk.NoError(errStore)

	if !validDatasetNameRegexp.MatchString(name) {
		return dataset.Dataset{}, fmt.Errorf("Improper dataset name: %s", in)
	}

	return dataset.NewDataset(db, name), nil
}

//ParseObject takes a colon-delineated string indicating a Database and an object from the Database to return. It also indicates whether the retun value is a dataset or a ref using a boolean. Supported syntax includes
//<database>:<dataset>
//<database>:<ref>
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
