package user

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datastore"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

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

func TestCreateUser(t *testing.T) {
	assert := assert.New(t)
	ds := datastore.NewDataStore(&chunks.MemoryStore{})
	CreateUser(ds, "foo@bar.com")
	users := ds.GetRoots().Any().(types.Map).Get(types.NewString("value")).(types.Set)
	assert.EqualValues(1, users.Len())
	user := users.Any().(types.Map)
	assert.True(types.NewString("foo@bar.com").Equals(user.Get(types.NewString("email"))))
}
