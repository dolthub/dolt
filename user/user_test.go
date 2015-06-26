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
	ms := &chunks.MemoryStore{}
	oldDs := datastore.NewDataStore(ms, ms)

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
	assert.Equal("foo@bar.com", users.Any().Email().String())
}

func TestGetUser(t *testing.T) {
	assert := assert.New(t)
	ms := &chunks.MemoryStore{}
	ds := datastore.NewDataStore(ms, ms)
	users := GetUsers(ds)
	user := GetUser(users, "foo@bar.com")
	assert.Equal(User{}, user)
	users = InsertUser(users, "foo@bar.com")
	user = GetUser(users, "foo@bar.com")
	assert.Equal("foo@bar.com", user.Email().String())
}

func TestSetAppRoot(t *testing.T) {
	assert := assert.New(t)
	users := InsertUser(NewUserSet(), "foo@bar.com")
	users = SetAppRoot(users, "foo@bar.com", "app", types.Int32(42))
	assert.EqualValues(1, users.Len())
	assert.True(types.Int32(42).Equals(users.Any().Apps().Any().Root()))
	assert.True(types.Int32(42).Equals(GetAppRoot(users, "foo@bar.com", "app")))
}
