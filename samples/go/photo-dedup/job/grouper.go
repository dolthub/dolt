// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package job

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/samples/go/photo-dedup/dhash"
	"github.com/attic-labs/noms/samples/go/photo-dedup/model"
)

// photoGrouper is a data structure used to group similar photos into PhotoGroups
//
// The current implementation is a simple map. Photo inserts are O(n^2).
// TODO: Replace the map with VP/MVP tree (https://en.wikipedia.org/wiki/Vantage-point_tree).
type photoGrouper struct {
	groups         map[model.ID]*group
	threshold      int
	photoCount     int
	duplicateCount int
}

type group struct {
	id     model.ID
	dhash  dhash.Hash
	photos map[*model.Photo]bool
}

func newPhotoGrouper(threshold int) *photoGrouper {
	return &photoGrouper{make(map[model.ID]*group), threshold, 0, 0}
}

func newGroup(photo *model.Photo) *group {
	photos := map[*model.Photo]bool{photo: true}
	return &group{model.NewAtticID(), photo.Dhash, photos}
}

func (g *photoGrouper) insertGroup(pg *group) {
}

// insertPhoto places the photo into an existing group if there is one that contains
// duplicate photos. Otherwise it creates a new group.
//
// The current implementation is a brute force n^2 comparision. A more efficient
// implementation would be to build an VP/MVP tree. A VP tree is a binary search
// tree that works in a geometric space. Each node defines a center point and a
// radius. Dhashes within the radius can be found to the left; those outside the
// radius can be found to the right. An MVP is the k-tree equivalent.
func (g *photoGrouper) insertPhoto(photo *model.Photo) {
	for _, group := range g.groups {
		if group.dhash != dhash.NilHash {
			if dhash.Distance(photo.Dhash, group.dhash) < g.threshold {
				if _, ok := group.photos[photo]; !ok {
					group.photos[photo] = true
					g.duplicateCount++
					g.photoCount++
				}
				return
			}
		}
	}
	status.Printf("Grouping - %d duplicates found in %d photos", g.duplicateCount, g.photoCount)
	new := newGroup(photo)
	g.groups[new.id] = new
	g.photoCount++
}

// iterGroups iterator through all the photo groups
func (g *photoGrouper) iterGroups(cb func(pg *model.PhotoGroup)) {
	for _, group := range g.groups {
		cover, rest := pickCover(group)
		cb(model.NewPhotoGroup(group.id, group.dhash, cover, rest))
	}
}

// Dumb implementation for now. Ultimately, there should be another job that picks the best photo.
func pickCover(pg *group) (*model.Photo, map[*model.Photo]bool) {
	d.Chk.True(len(pg.photos) > 0)
	var cover *model.Photo
	rest := map[*model.Photo]bool{}
	i := 0
	for p, _ := range pg.photos {
		if i == 0 {
			cover = p
		} else {
			rest[p] = true
		}
		i += 1
	}
	return cover, rest
}
