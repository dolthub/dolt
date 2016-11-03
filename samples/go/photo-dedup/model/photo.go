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

type Photo struct {
	Id    ID
	Sizes map[struct {
		Width  int
		Height int
	}]string
	Dhash dhash.Hash   `noms:"-"` // TODO: replace with optional field support
	Orig  types.Struct `noms:"-"` // TODO: replace with value preservation support
}

func UnmarshalPhoto(value types.Value) (*Photo, bool) {
	d.Chk.NotNil(value)
	p := Photo{}
	err := marshal.Unmarshal(value, &p)
	if err != nil {
		if _, ok := err.(*marshal.UnmarshalTypeMismatchError); ok {
			return nil, false
		}
		d.Chk.NoError(err)
	}
	s := value.(types.Struct)
	if dv, ok := s.MaybeGet("dhash"); ok {
		p.Dhash, err = dhash.Parse(string(dv.(types.String)))
	}
	p.Orig = s
	return &p, true
}

func (p *Photo) IterSizes(cb func(width int, height int, url string)) {
	for k, v := range p.Sizes {
		cb(k.Width, k.Height, v)
	}
}

// This can be replaced with marshal.Marshal when value preservation is implemented
func (p *Photo) Marshal() types.Struct {
	nomsV, err := marshal.Marshal(*p)
	d.Chk.NoError(err)
	nomsS := nomsV.(types.Struct)
	final := p.Orig
	nomsS.Type().Desc.(types.StructDesc).IterFields(func(name string, t *types.Type) {
		v := nomsS.Get(name)
		final = final.Set(name, v)
	})
	final = final.Set("dhash", types.String(p.Dhash.String()))
	return final
}
