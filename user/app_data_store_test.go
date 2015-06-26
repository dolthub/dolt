package user

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestAppRootTracker(t *testing.T) {
	assert := assert.New(t)
	userEmail := "foo@bar.com"
	appId := "testapp.com"
	ms := &chunks.MemoryStore{}
	rootDs := datastore.NewDataStore(ms, ms)
	rootDs = CommitUsers(rootDs, InsertUser(NewUserSet(), userEmail))
	users := GetUsers(rootDs)
	assert.Equal(nil, GetAppRoot(users, userEmail, appId))

	art := &appRootTracker{rootDs, userEmail, appId}
	appDs := datastore.NewDataStore(ms, art)
	appRoot := types.NewString("Hello, AppRoot!")
	appDs = appDs.Commit(datastore.NewRootSet().Insert(
		datastore.NewRoot().SetParents(
			types.NewSet()).SetValue(
			appRoot)))
	assert.EqualValues(1, appDs.Roots().Len())
}
