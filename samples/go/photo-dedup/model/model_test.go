package model_test

import (
	"testing"
	"time"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/photo-dedup/model"
	"github.com/attic-labs/testify/assert"
)

func TestCommitMeta(t *testing.T) {
	commit := model.NewCommitMeta()
	strct := commit.Marshal()
	v := strct.Get("date").(types.String)
	date, err := time.Parse(time.RFC3339, string(v))
	assert.NoError(t, err)
	assert.True(t, date.Before(time.Now()))
}
