package user

import (
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
)

func GetUsers(ds datastore.DataStore) types.Set {
	if ds.Roots().Len() == 0 {
		return types.NewSet()
	} else {
		return ds.Roots().Any().(types.Map).Get(types.NewString("value")).(types.Set)
	}
}

func InsertUser(users types.Set, email string) types.Set {
	user := types.NewMap(
		types.NewString("$type"), types.NewString("noms.User"),
		types.NewString("email"), types.NewString(email),
		// TODO: Need nil Value so that we can put nil appRoot in here now?
	)

	// TODO: What if the user exists with a set appRoot? Need GetUser() check above.
	return users.Insert(user)
}

func CommitUsers(ds datastore.DataStore, users types.Set) datastore.DataStore {
	return ds.Commit(types.NewSet(
		types.NewMap(
			types.NewString("$type"), types.NewString("noms.Root"),
			types.NewString("parents"), ds.Roots(),
			types.NewString("value"), users)))
}

// TODO:
// - GetUser(), then update CreateUser() to check for that
// - SetUserAppRoot() (assume there's just one app for now)
// - New RootTracker impl that uses above
// - success?
