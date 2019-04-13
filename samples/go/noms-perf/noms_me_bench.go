package main

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
)

type NomsMEBench struct {
	me *types.MapEditor
}

func NewNomsMEBench() *NomsMEBench {
	ts := &chunks.TestStorage{}
	vrw := types.NewValueStore(ts.NewView())
	me := types.NewMap(vrw).Edit()

	return &NomsMEBench{me}
}

func (nmeb *NomsMEBench) GetName() string {
	return "noms map editor"
}

func (nmeb *NomsMEBench) AddEdits(nextEdit NextEdit) {
	k, v := nextEdit()

	for k != nil {
		nmeb.me = nmeb.me.Set(k, v)
		k, v = nextEdit()
	}
}

func (nmeb *NomsMEBench) SortEdits() {
	nmeb.me.SortStable()
}

func (nmeb *NomsMEBench) Map() {
	nmeb.me.Map()
}
