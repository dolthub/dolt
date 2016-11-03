// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package job

import (
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"net/http"
	"runtime"
	"sync"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/walk"
	"github.com/attic-labs/noms/samples/go/photo-dedup/dhash"
	"github.com/attic-labs/noms/samples/go/photo-dedup/model"
	"github.com/attic-labs/noms/go/util/verbose"
)

// HashPhotosJob adds a dhash field to every photo in photoSets and commits them to
// a new Photo set in outDS. The dhash is used in turn by the DeduplicatePhotosJob
// to group similar photos into PhotoGroups.
func HashPhotosJob(db datas.Database, photoSets []types.Value, outDS datas.Dataset) error {
	return commitHashedPhotos(db, outDS, hashPhotos(db, photoSets))
}

// hashPhotos adds a dhash to each photo in photoSets and delivers them on the returned channel
func hashPhotos(db datas.Database, photoSets []types.Value) <-chan types.Struct {
	numWorkers := runtime.NumCPU() * 4
	toHash := make(chan *model.Photo, numWorkers)
	hashed := make(chan types.Struct, numWorkers)

	go func() {
		for _, set := range photoSets {
			walk.WalkValues(set, db, func(cv types.Value) (stop bool) {
				if photo, ok := model.UnmarshalPhoto(cv); ok {
					toHash <- photo
				}
				return false
			})
		}
		close(toHash)
	}()

	fmt.Print("Downloading and hashing photos...\n")
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for photo := range toHash {
				withHash, err := addHashToPhoto(photo)
				if err != nil {
					if verbose.Verbose() {
						fmt.Printf("\nSkipping: %s\n", err)
					}
					hashed <- photo.Marshal()
				} else {
					hashed <- withHash.Marshal()
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(hashed)
	}()
	return hashed
}

func addHashToPhoto(photo *model.Photo) (*model.Photo, error) {
	url := pickBestImage(photo)
	if url == "" {
		return nil, fmt.Errorf("No URL found for photo %s", photo.Id)
	}
	res, err := http.Get(string(url))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	img, _, err := image.Decode(res.Body)
	if err != nil {
		if err == image.ErrFormat {
			err = fmt.Errorf("%s: unknown format", url)
		}
		return nil, err
	}
	photo.Dhash = dhash.New(img)
	return photo, nil
}

// pickBestImage returns the image URL corresponding to the size
// closest to but not below the ideal size. If there is no image
// large enough, it returns the next smallest image.
func pickBestImage(photo *model.Photo) string {
	idealSize := 240 * 240

	closestURL := ""
	closestDist := int(math.MaxInt64)

	closestBelowURL := ""
	closestBelowDist := int(-math.MaxInt64)

	photo.IterSizes(func(w int, h int, url string) {
		dist := w*h - idealSize
		if dist >= 0 && dist < closestDist {
			closestURL = url
			closestDist = dist
		} else if dist < 0 && dist > closestBelowDist {
			closestBelowURL = url
			closestBelowDist = dist
		}
	})
	if closestDist < int(math.MaxInt64) {
		return closestURL
	}
	return closestBelowURL
}

// commitHashedPhotos reads the annotated photos off the hashPhotos channel and commits
// them to the new ds
func commitHashedPhotos(db datas.Database, ds datas.Dataset, hashedPhotos <-chan types.Struct) error {
	newSet := types.NewGraphBuilder(db, types.SetKind, true)
	count := 0
	for photo := range hashedPhotos {
		count += 1
		status.Printf("Hashing - %d photos processed", count)
		newSet.SetInsert(nil, photo)
	}
	status.Done()
	fmt.Printf("Committing %d hashed Photos\n", count)
	commit := newSet.Build()
	meta := model.NewCommitMeta().Marshal()
	_, err := db.Commit(ds, commit, datas.CommitOptions{Meta: meta})
	return err
}
