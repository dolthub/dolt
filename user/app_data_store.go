package user

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

// Returns a datastore whose chunks are stored in rootStore, but whose root is associated with the specified user (and app in the future).
func NewAppDataStore(rootStore datastore.DataStore, userEmail string) datastore.DataStore {
	return datastore.NewDataStore(rootStore, &appRootTracker{rootStore, userEmail})
}

type appDataStoreFlags struct {
	chunks.Flags
	userEmail *string
}

func AppDataFlags() appDataStoreFlags {
	return appDataStoreFlags{
		chunks.NewFlags(),
		flag.String("user-email", "", "email address of user to store data for"),
	}
}

func (f appDataStoreFlags) CreateStore() *datastore.DataStore {
	if *f.userEmail == "" {
		return nil
	}
	cs := f.Flags.CreateStore()
	if cs == nil {
		return nil
	}

	// Blech, kinda sucks to typecast to RootTracker, but we know that all the implementations of ChunkStore that implement it.
	rootDataStore := datastore.NewDataStore(cs, cs.(chunks.RootTracker))

	// For now, we just create the user here. In real life, user creation should be done elsewhere of course.
	rootDataStore = CommitUsers(rootDataStore, InsertUser(GetUsers(rootDataStore), *f.userEmail))

	ds := NewAppDataStore(rootDataStore, *f.userEmail)
	return &ds
}

type appRootTracker struct {
	rootStore datastore.DataStore
	userEmail string
	// Future: appId
}

func (rt *appRootTracker) Root() ref.Ref {
	user := GetAppRoot(GetUsers(rt.rootStore), rt.userEmail)
	if user == nil {
		return ref.Ref{}
	} else {
		return user.Ref()
	}
}

func (rt *appRootTracker) UpdateRoot(current, last ref.Ref) bool {
	if last != rt.Root() {
		return false
	}

	// BUG 11: Sucks to have to read the app root here in order to commit.
	appRoot := enc.MustReadValue(current, rt.rootStore)

	rt.rootStore = CommitUsers(rt.rootStore, SetAppRoot(GetUsers(rt.rootStore), rt.userEmail, appRoot))
	return true
}
