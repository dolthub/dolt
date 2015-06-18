package user

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

/*
func TestInitDataStore(t *testing.T) {
	assert := assert.New(t)
	ds := datastore.NewDataStore(&chunks.MemoryStore{})
	assert.EqualValues(0, ds.GetRoots().Len())
	InitDataStore(ds)
	assert.EqualValues(1, ds.GetRoots().Len())
	users := ds.GetRoots().Any().(types.Map).Get(types.NewString("value")).(types.Set)
	assert.EqualValues(0, users.Len())

	// It's OK to call it twice -- the commit will just fail.
	InitDataStore(ds)
}
*/

func TestInsertUser(t *testing.T) {
	assert := assert.New(t)
	oldDs := datastore.NewDataStore(&chunks.MemoryStore{})

	oldUsers := GetUsers(oldDs)
	assert.EqualValues(0, oldUsers.Len())
	users := InsertUser(oldUsers, "foo@bar.com")
	assert.EqualValues(0, oldUsers.Len())
	assert.EqualValues(1, users.Len())
	ds := CommitUsers(oldDs, users)
	users = GetUsers(ds)
	assert.EqualValues(0, GetUsers(oldDs).Len())
	assert.EqualValues(1, users.Len())

	assert.EqualValues(1, users.Len())
	user := users.Any().(types.Map)
	assert.True(types.NewString("foo@bar.com").Equals(user.Get(types.NewString("email"))))
}
