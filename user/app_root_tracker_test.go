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
	ms := &chunks.MemoryStore{}
	rootDs := datastore.NewDataStore(ms, ms)
	rootDs = CommitUsers(rootDs, InsertUser(types.NewSet(), userEmail))
	users := GetUsers(rootDs)
	assert.Equal(nil, GetAppRoot(users, userEmail))

	art := &AppRootTracker{rootDs, userEmail}
	appDs := datastore.NewDataStore(ms, art)
	appRoot := types.NewString("Hello, AppRoot!")
	appDs = appDs.Commit(types.NewSet(types.NewMap(
		types.NewString("$type"), types.NewString("noms.Root"),
		types.NewString("parents"), types.NewSet(),
		types.NewString("value"), appRoot)))
	assert.EqualValues(1, appDs.Roots().Len())
}
