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
}
