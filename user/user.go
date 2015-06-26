package user

import (
	"github.com/attic-labs/noms/datastore"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/types"
)

//go:generate go run gen/types.go -o types.go

func GetUsers(ds datastore.DataStore) UserSet {
	if ds.Roots().Empty() {
		return NewUserSet()
	} else {
		// BUG 13: We don't ever want to branch the user database. Currently we can't avoid that, but we should change DataStore::Commit() to support that mode of operation.
		Chk.EqualValues(1, ds.Roots().Len())
		return UserSetFromVal(ds.Roots().Any().Value())
	}
}

func InsertUser(users UserSet, email string) UserSet {
	if GetUser(users, email) != (User{}) {
		return users
	}
	return users.Insert(
		NewUser().SetEmail(types.NewString(email)).SetApps(NewAppSet()))
}

func CommitUsers(ds datastore.DataStore, users UserSet) datastore.DataStore {
	return ds.Commit(datastore.NewRootSet().Insert(
		datastore.NewRoot().SetParents(
			ds.Roots().NomsValue()).SetValue(
			users.NomsValue())))
}

func GetUser(users UserSet, email string) (r User) {
	// n queries
	// could parallelize using go routines. we actually want all the users in memory so we could just get them at GetUsers()
	users.Iter(func(v User) (stop bool) {
		if v.Email().String() == email {
			r = v
			stop = true
		}
		return
	})
	return
}

func GetApp(apps AppSet, appId string) (r App) {
	apps.Iter(func(app App) (stop bool) {
		if app.Id().String() == appId {
			r = app
			stop = true
		}
		return
	})
	return
}

func GetAppRoot(users UserSet, userEmail, appId string) types.Value {
	user := GetUser(users, userEmail)
	Chk.NotEmpty(user, "Unknown user: %s", userEmail)
	app := GetApp(user.Apps(), appId)
	if app == (App{}) {
		return nil
	}
	return app.Root()
}

func SetAppRoot(users UserSet, userEmail, appId string, val types.Value) UserSet {
	user := GetUser(users, userEmail)
	Chk.NotEmpty(user, "Unknown user: %s", userEmail)
	apps := user.Apps()
	app := GetApp(apps, appId)

	return users.Remove(user).Insert(
		user.SetApps(
			apps.Remove(app).Insert(
				NewApp().
					SetId(types.NewString(appId)).
					SetRoot(val))))
}
