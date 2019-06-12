package dbfactory

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
func TestCreateFileDB(t *testing.T) {
	ctx := context.Background()

	db, err := CreateDB(ctx, "file://testdata/", nil)

	assert.NoError(t, err)

	datasets := db.Datasets(ctx)

	assert.Equal(t, int(datasets.Len()), 2)

	master, masterOK := datasets.MaybeGet(ctx, types.String("refs/heads/master"))
	assert.True(t, masterOK)

	masterVal := master.(types.Ref).TargetValue(ctx, db)
	assert.NotNil(t, masterVal)

	create, createOK := datasets.MaybeGet(ctx, types.String("refs/internal/create"))
	assert.True(t, createOK)

	createVal := create.(types.Ref).TargetValue(ctx, db)
	assert.NotNil(t, createVal)

	_, fakeOK := datasets.MaybeGet(ctx, types.String("refs/heads/fake"))
	assert.False(t, fakeOK)
}
*/

func TestCreateMemDB(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDB(ctx, "mem://", nil)

	assert.NoError(t, err)
	assert.NotNil(t, db)
}
