package user

import (
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

// Implements chunks.RootTracker by storing the root ref in the user tree. This is how we provide a DataStore abstraction to apps that makes it look like they are alone in the universe.
type AppRootTracker struct {
	ds        datastore.DataStore
	userEmail string
	// Future: appId
}

func (rt *AppRootTracker) Root() ref.Ref {
	user := GetAppRoot(GetUsers(rt.ds), rt.userEmail)
	if user == nil {
		return ref.Ref{}
	} else {
		return user.Ref()
	}
}

func (rt *AppRootTracker) UpdateRoot(current, last ref.Ref) bool {
	if last != rt.Root() {
		return false
	}

	// BUG 11: Sucks to have to read the app root here in order to commit.
	appRoot := enc.MustReadValue(current, rt.ds)

	rt.ds = CommitUsers(rt.ds, SetAppRoot(GetUsers(rt.ds), rt.userEmail, appRoot))
	return true
}
