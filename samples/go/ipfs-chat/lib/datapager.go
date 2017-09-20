// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"fmt"

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

var dp dataPager

func (dp *dataPager) Close() {
	dp.doneChan <- struct{}{}
}

func (dp *dataPager) IsEmpty() bool {
	return dp.msgKeyChan == nil && dp.doneChan == nil
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
