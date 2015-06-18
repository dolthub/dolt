package user

import (
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
)

func InitDataStore(ds datastore.DataStore) {
	if ds.GetRoots().Len() > 0 {
		return
	}
	ds.Commit(types.NewSet(
		types.NewMap(
			types.NewString("$type"),
			types.NewString("noms.Root"),
			types.NewString("parents"),
			types.NewSet(),
			types.NewString("value"),
			types.NewSet(), // empty set of noms.User
		),
	))
}
