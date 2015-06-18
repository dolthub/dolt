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
	if GetUser(users, email) != nil {
		return users
	}
	user := types.NewMap(
		types.NewString("$type"), types.NewString("noms.User"),
		types.NewString("email"), types.NewString(email),
		// TODO: Need nil Value so that we can put nil appRoot in here now?
	)
	return users.Insert(user)
}

func CommitUsers(ds datastore.DataStore, users types.Set) datastore.DataStore {
	return ds.Commit(types.NewSet(
		types.NewMap(
			types.NewString("$type"), types.NewString("noms.Root"),
			types.NewString("parents"), ds.Roots(),
			types.NewString("value"), users)))
}

func GetUser(users types.Set, email string) (r types.Map) {
	users.Iter(func(v types.Value) (stop bool) {
		if v.(types.Map).Get(types.NewString("email")).Equals(types.NewString(email)) {
			r = v.(types.Map)
			stop = true
		}
		return
	})
	return
}

// TODO:
// - GetUser(), then update CreateUser() to check for that
// - SetUserAppRoot() (assume there's just one app for now)
// - New RootTracker impl that uses above
// - success?
