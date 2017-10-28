// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"fmt"
	"strings"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
)

type dataPager struct {
	dataset    datas.Dataset
	msgKeyChan chan types.String
	doneChan   chan struct{}
	msgMap     types.Map
	terms      []string
}

func NewDataPager(ds datas.Dataset, mkChan chan types.String, doneChan chan struct{}, msgs types.Map, terms []string) *dataPager {
	return &dataPager{
		dataset:    ds,
		msgKeyChan: mkChan,
		doneChan:   doneChan,
		msgMap:     msgs,
		terms:      terms,
	}
}

func (dp *dataPager) Close() {
	dp.doneChan <- struct{}{}
}

func (dp *dataPager) Next() (string, bool) {
	msgKey := <-dp.msgKeyChan
	if msgKey == "" {
		return "", false
	}
	nm := dp.msgMap.Get(msgKey)

	var m Message
	err := marshal.Unmarshal(nm, &m)
	if err != nil {
		return fmt.Sprintf("ERROR: %s", err.Error()), true
	}

	s1 := fmt.Sprintf("%s: %s", m.Author, m.Body)
	s2 := highlightTerms(s1, dp.terms)
	return s2, true
}

func (dp *dataPager) Prepend(lines []string, target int) ([]string, bool) {
	new := []string{}
	m, ok := dp.Next()
	if !ok {
		return lines, false
	}
	for ; ok && len(new) < target; m, ok = dp.Next() {
		new1 := strings.Split(m, "\n")
		new = append(new1, new...)
	}
	return append(new, lines...), true
}
