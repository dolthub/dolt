// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"golang.org/x/net/html"
)

var (
	character = ""
	msgs      = []Message{}
)

func runImport(dir, dsSpec string) error {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if path == dir {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".html") {
			return nil
		}
		fmt.Println("importing:", path)
		f, err := os.Open(path)
		d.Chk.NoError(err)
		n, err := html.Parse(f)
		d.Chk.NoError(err)
		extractDialog(n)
		return nil
	})

	if len(msgs) == 0 {
		return errors.New("Failed to import any data")
	} else {
		fmt.Println("Imported", len(msgs), "messages")
	}

	sp, err := spec.ForDataset(dsSpec)
	d.CheckErrorNoUsage(err)
	ds := sp.GetDataset()
	ds, err = InitDatabase(ds)
	d.PanicIfError(err)

	fmt.Println("Creating msg map")
	kvPairs := []types.Value{}
	for _, msg := range msgs {
		kvPairs = append(kvPairs, types.String(msg.ID()), marshal.MustMarshal(msg))
	}
	m := types.NewMap(kvPairs...)

	fmt.Println("Creating index")
	ti := NewTermIndex(types.NewMap()).Edit()
	for _, msg := range msgs {
		terms := GetTerms(msg)
		ti.InsertAll(terms, types.String(msg.ID()))
	}
	termDocs := ti.Value(nil).TermDocs

	userpat := regexp.MustCompile(`^[a-zA-Z][a-zA-Z\s]*\d*$`)
	fmt.Println("Creating users")
	usermap := map[string]struct{}{}
outer:
	for _, msg := range msgs {
		name := strings.TrimSpace(msg.Author)
		if !userpat.MatchString(name) {
			continue outer
		}
		usermap[name] = struct{}{}
	}

	users := []string{}
	for k, _ := range usermap {
		users = append(users, k)
	}
	sort.Strings(users)
	fmt.Println("Committing data")
	root := Root{Messages: m, Index: termDocs, Users: users}
	_, err = ds.Database().CommitValue(ds, marshal.MustMarshal(root))
	return err
}

func extractDialog(n *html.Node) {
	if c := characterName(n); c != "" {
		//fmt.Println("Character:", character)
		character = c
		return
	}
	if character != "" && n.Type == html.TextNode {
		//fmt.Println("Dialog:", strings.TrimSpace(n.Data))
		msg := Message{
			Ordinal:    uint64(len(msgs)),
			Author:     character,
			Body:       strings.TrimSpace(n.Data),
			ClientTime: datetime.Now(),
		}
		msgs = append(msgs, msg)
		character = ""
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extractDialog(c)
	}
}

func characterName(n *html.Node) string {
	if n.Type != html.ElementNode ||
		n.Data != "b" ||
		n.FirstChild == nil {
		return ""
	}

	if hasSpaces, _ := regexp.MatchString(`^\s+[^\s]`, n.FirstChild.Data); !hasSpaces {
		return ""
	}
	return strings.TrimSpace(n.FirstChild.Data)
}
