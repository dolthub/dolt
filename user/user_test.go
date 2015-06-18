package user

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

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

func TestGetUser(t *testing.T) {
	assert := assert.New(t)
	ds := datastore.NewDataStore(&chunks.MemoryStore{})
	users := GetUsers(ds)
	user := GetUser(users, "foo@bar.com")
	assert.Nil(user)
	users = InsertUser(users, "foo@bar.com")
	user = GetUser(users, "foo@bar.com")
	assert.True(user.Get(types.NewString("email")).Equals(types.NewString("foo@bar.com")))
}

func TestSetAppRoot(t *testing.T) {
	assert := assert.New(t)
	users := InsertUser(types.NewSet(), "foo@bar.com")
	users = SetAppRoot(users, "foo@bar.com", types.Int32(42))
	assert.EqualValues(1, users.Len())
	assert.True(types.Int32(42).Equals(users.Any().(types.Map).Get(types.NewString("appRoot"))))
}
