package model

import (
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
)

type CommitMeta struct {
	Date string
}

func NewCommitMeta() CommitMeta {
	return CommitMeta{
		time.Now().Format(time.RFC3339),
	}
}

func (c CommitMeta) Marshal() types.Struct {
	v, err := marshal.Marshal(c)
	d.Chk.NoError(err)
	return v.(types.Struct)
}
