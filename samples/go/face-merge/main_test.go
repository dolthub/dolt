// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestBasics(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
	sp spec.Spec
}
type Face struct {
	Name       string
	X, Y, W, H float64
}
type FaceCenter struct {
	Name string
	X, Y float64
}

type FaceRect struct {
	X, Y, W, H float64
}

type Date struct {
	NsSinceEpoch int
}

type Photo struct {
	Title         string
	Tags          types.Set
	FacesRect     types.Set
	FacesCentered types.Set
	Sizes         map[struct {
		Width  int
		Height int
	}]string
	DateTaken     Date
	DatePublished Date
	DateUpdated   Date
}

type PhotoOutput struct {
	Title string
	Tags  types.Set
	Faces types.Set
	Sizes map[struct {
		Width  int
		Height int
	}]string
	DateTaken     Date
	DatePublished Date
	DateUpdated   Date
}

func getFaceCenter(x, y int, name string) types.Value {
	val, err := marshal.Marshal(FaceCenter{
		name, float64(x), float64(y),
	})
	d.Chk.NoError(err)
	return val
}

func getFaceRect(x, y, w, h int) types.Value {
	val, err := marshal.Marshal(FaceRect{
		float64(x), float64(y), float64(w), float64(h),
	})
	d.Chk.NoError(err)
	return val
}

func getPhoto(title string, faceRects types.Set, faceCenters types.Set) Photo {
	return Photo{
		Title: title,
		Tags:  types.NewSet(),
		Sizes: map[struct{ Width, Height int }]string{
			{100, 100}: "100.jpg"},
		DateTaken:     Date{10},
		DatePublished: Date{10 + 1},
		DateUpdated:   Date{10 + 2},
		FacesRect:     faceRects,
		FacesCentered: faceCenters,
	}
}

func getFace(x, y, w, h types.Number, name types.String) types.Struct {
	return types.NewStruct("", types.StructData{
		"x":    x,
		"y":    y,
		"w":    w,
		"h":    h,
		"name": name,
	})
}

func getPhotoOutput(photo Photo, faces types.Set) PhotoOutput {
	return PhotoOutput{
		Title:         photo.Title,
		Tags:          photo.Tags,
		Sizes:         photo.Sizes,
		DateTaken:     photo.DateTaken,
		DatePublished: photo.DatePublished,
		DateUpdated:   photo.DateUpdated,
		Faces:         faces,
	}
}

func (s *testSuite) SetupTest() {
	var err error
	s.sp, err = spec.ForDataset(fmt.Sprintf("ldb:%s::test", s.LdbDir))
	s.NoError(err)
}

func (s *testSuite) TearDownTest() {
	s.sp.Close()
}

func (s *testSuite) TestMerge() {
	photos := []Photo{}
	faceRects1 := types.NewSet()
	faceRects2 := types.NewSet()
	faceCenters1 := types.NewSet()
	faceCenters2 := types.NewSet()
	faces1 := types.NewSet()
	faces2 := types.NewSet()

	//Intersects with face rectangle
	faceRects1 = faceRects1.Insert(getFaceRect(10, 10, 20, 20))
	faceCenters1 = faceCenters1.Insert(getFaceCenter(15, 15, "Jane"))
	faces1 = faces1.Insert(getFace(10, 10, 20, 20, "Jane"))

	//Intersects with face rectangle
	faceRects1 = faceRects1.Insert(getFaceRect(40, 40, 20, 20))
	faceCenters1 = faceCenters1.Insert(getFaceCenter(45, 45, "Janet"))
	faces1 = faces1.Insert(getFace(40, 40, 20, 20, "Janet"))

	//Does not intersect with face rectangle
	faceRects1 = faceRects1.Insert(getFaceRect(70, 70, 20, 20))
	faceCenters1 = faceCenters1.Insert(getFaceCenter(95, 95, "John"))

	//Does not intersect with face rectangle
	faceRects2 = faceRects2.Insert(getFaceRect(50, 50, 20, 20))
	faceCenters2 = faceCenters2.Insert(getFaceCenter(10, 10, "Joe"))

	photo1 := getPhoto("photo1", faceRects1, faceCenters1)
	photo2 := getPhoto("photo2", faceRects2, faceCenters2)

	photos = append(photos, photo1)
	photos = append(photos, photo2)

	photoOutput1 := getPhotoOutput(photo1, faces1)
	photoOutput2 := getPhotoOutput(photo2, faces2)

	v, err := marshal.Marshal(photos)
	s.NoError(err)
	_, err = s.sp.GetDatabase().CommitValue(s.sp.GetDataset(), v)
	s.NoError(err)

	verifyOutput := func(photoA types.Struct, photoB types.Struct) {
		s.Equal(photoA.Get("title").Equals(photoB.Get("title")), true)
		s.Equal(photoA.Get("faces").Equals(photoB.Get("faces")), true)
		s.Equal(photoA.Get("sizes").Equals(photoB.Get("sizes")), true)
		s.Equal(photoA.Get("dateTaken").Equals(photoB.Get("dateTaken")), true)
		s.Equal(photoA.Get("datePublished").Equals(photoB.Get("datePublished")), true)
		s.Equal(photoA.Get("dateUpdated").Equals(photoB.Get("dateUpdated")), true)
		s.Equal(photoA.Get("tags").Equals(photoB.Get("tags")), true)
	}

	stdo, _ := s.MustRun(main, []string{"--out-ds", "idx", "--db", s.LdbDir, "test"})
	fmt.Println(stdo)
	sp, err := spec.ForDataset(fmt.Sprintf("%s::idx", s.LdbDir))
	s.NoError(err)
	val := types.Set{}
	marshal.Unmarshal(sp.GetDataset().HeadValue(), &val)

	val.IterAll(func(v types.Value) {
		var testOutput types.Value
		if v.(types.Struct).Get("title").(types.String) == "photo1" {
			testOutput, _ = marshal.Marshal(photoOutput1)
		} else if v.(types.Struct).Get("title").(types.String) == "photo2" {
			testOutput, _ = marshal.Marshal(photoOutput2)
		} else {
			panic("Invalid test state reached ")
		}
		verifyOutput(testOutput.(types.Struct), v.(types.Struct))

	})
}
