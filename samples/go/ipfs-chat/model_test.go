// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"
)

func TestBasics(t *testing.T) {
	a := assert.New(t)
	db := datas.NewDatabase(chunks.NewMemoryStoreFactory().CreateStore(""))
	ds := db.GetDataset("foo")
	ml, err := getAllMessages(ds)
	a.NoError(err)
	a.Equal(0, len(ml))

	ds, err = AddMessage("body1", "aa", time.Unix(0, 0), ds)
	a.NoError(err)
	ml, err = getAllMessages(ds)
	a.NoError(err)
	expected := []Message{
		Message{
			Author:     "aa",
			Body:       "body1",
			ClientTime: datetime.DateTime{time.Unix(0, 0)},
			Ordinal:    0,
		},
	}
	a.Equal(expected, ml)

	ds, err = AddMessage("body2", "bob", time.Unix(1, 0), ds)
	a.NoError(err)
	ml, err = getAllMessages(ds)
	expected = append(
		[]Message{
			Message{
				Author:     "bob",
				Body:       "body2",
				ClientTime: datetime.DateTime{time.Unix(1, 0)},
				Ordinal:    1,
			},
		},
		expected...,
	)
	a.NoError(err)
	a.Equal(expected, ml)
}

func getAllMessages(ds datas.Dataset) (r []Message, err error) {
	doneChan := make(chan struct{})
	mm, keys, _ := ListMessages(ds, nil, doneChan)
	for k := range keys {
		mv := mm.Get(k)
		var m Message
		marshal.MustUnmarshal(mv, &m)
		r = append(r, m)
	}
	doneChan <- struct{}{}
	return r, nil
}
