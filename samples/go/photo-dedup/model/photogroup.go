// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package model

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/photo-dedup/dhash"
)

type PhotoGroup struct {
	ID     ID `noms:"id"`
	Dhash  dhash.Hash
	Cover  *Photo          `noms:"-"` // ignore tag required until ref support in marshalling
	Photos map[*Photo]bool `noms:"-"` // ignore tag required until ref support in marshalling
}

func NewPhotoGroup(id ID, dhash dhash.Hash, cover *Photo, photos map[*Photo]bool) *PhotoGroup {
	return &PhotoGroup{id, dhash, cover, photos}
}

// TODO: replace with simple marshalling when ref support is implemented
func (pg *PhotoGroup) Marshal() types.Struct {
	v, err := marshal.Marshal(*pg)
	d.Chk.NoError(err)
	s := v.(types.Struct)
	s = s.Set("cover", pg.Cover.Marshal())
	refs := []types.Value{}
	for p, _ := range pg.Photos {
		refs = append(refs, p.Marshal())
	}
	s = s.Set("photos", types.NewSet(refs...))
	return s
}
