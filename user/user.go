package user

import (
	"github.com/attic-labs/noms/datastore"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/types"
)

func commit(ds datastore.DataStore, oldRoots types.Set, users types.Set) {
	ds.Commit(types.NewSet(
		types.NewMap(
			types.NewString("$type"), types.NewString("noms.Root"),
			types.NewString("parents"), oldRoots,
			types.NewString("value"), users)))
}

func InitDataStore(ds datastore.DataStore) types.Set {
	roots := ds.GetRoots()
	if roots.Len() == 0 {
		commit(ds, roots, types.NewSet())
		roots = ds.GetRoots()
	}
	Chk.EqualValues(1, roots.Len())
	return roots
}

func CreateUser(ds datastore.DataStore, email string) {
	roots := InitDataStore(ds)
	user := types.NewMap(
		types.NewString("$type"), types.NewString("noms.User"),
		types.NewString("email"), types.NewString(email),
		// TODO: Need nil Value so that we can put nil appRoot in here now?
	)

	// TODO: What if the user exists with a set appRoot? Need GetUser() check above.
	users := roots.Any().(types.Map).Get(types.NewString("value")).(types.Set)
	users = users.Insert(user)
	commit(ds, roots, users)
}
