package main

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/stretchr/testify/assert"
)

type fakeFlickrAPI struct {
	methods map[string]string
}

func (api fakeFlickrAPI) Call(method string, response interface{}, args *map[string]string) error {
	if responseJson, ok := api.methods[method]; ok {
		return json.Unmarshal([]byte(responseJson), response)
	} else {
		return errors.New("unknown method " + method)
	}
}

func TestGetAlbums(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()
	testDs := dataset.NewDataset(datas.NewDataStore(cs), "test")
	ds = &testDs
	methods := map[string]string{
		"flickr.photosets.getList": `{
			"photosets": {
				"photoset": [
					{
						"id": "42",
						"photos": 2,
						"title": {
							"_content": "My Photoset"
						},
						"description": {
							"_content": ""
						}
					}
				]
			}
		}`,
		"flickr.photosets.getInfo": `{
			"photoset": {
				"id": "42",
				"username": "me",
				"photos": 2,
				"title": {
					"_content": "My Photoset"
				},
				"description": {
					"_content": ""
				}
			}
		}`,
		"flickr.photosets.getPhotos": `{
			"photoset": {
				"id": "42",
				"photo": [
					{
						"id": "0",
						"title": "_0",
						"datetaken": "2011-08-13 04:54:40",
						"url_s": "https:\/\/staticflickr.com\/0\/0.jpg",
						"height_s": "159",
						"width_s": "240",
						"url_m": "https:\/\/staticflickr.com\/0\/1.jpg",
						"height_m": "332",
						"width_m": "500",
						"url_l": "https:\/\/staticflickr.com\/0\/2.jpg",
						"height_l": "679",
						"width_l": "1024",
						"url_o": "https:\/\/staticflickr.com\/0\/3.jpg",
						"height_o": "679",
						"width_o": "1024",
						"longitude": 0,
						"latitude": 0
					},
					{
						"id": "1",
						"title": "_1",
						"datetaken": "2011-12-13 04:51:08",
						"url_s": "https:\/\/staticflickr.com\/1\/0.jpg",
						"height_s": "159",
						"width_s": "240",
						"url_m": "https:\/\/staticflickr.com\/1\/1.jpg",
						"height_m": "332",
						"width_m": "500",
						"url_l": "https:\/\/staticflickr.com\/1\/2.jpg",
						"height_l": "679",
						"width_l": "1024",
						"url_o": "https:\/\/staticflickr.com\/1\/3.jpg",
						"height_o": "6790",
						"width_o": "10240",
						"latitude": 48.8582641,
						"longitude": 2.2923184
					}
				],
				"title": "My Photoset"
			}
		}`,
	}

	albums := getAlbums(fakeFlickrAPI{methods})
	assert.Equal(uint64(1), albums.Len())

	album := albums.Get("42")
	assert.Equal("42", album.Id())
	assert.Equal("My Photoset", album.Title())

	photos := album.Photos().TargetValue(cs)
	assert.Equal(uint64(2), photos.Len())

	var photo0, photo1 RemotePhoto
	photos.IterAll(func(photo RefOfRemotePhoto) {
		p := photo.TargetValue(cs)
		switch id := p.Id(); id {
		case "0":
			photo0 = p
		case "1":
			photo1 = p
		default:
			panic("unexpected photo " + id)
		}
	})

	assert.Equal("0", photo0.Id())
	assert.Equal("_0", photo0.Title())
	assert.Equal(int64(1313236480000), photo0.Date().MsSinceEpoch())
	assert.Equal(float32(0), photo0.Geoposition().Latitude())
	assert.Equal(float32(0), photo0.Geoposition().Longitude())
	assert.Equal(uint64(3), photo0.Sizes().Len()) // two of the images are the same
	assert.Equal(uint64(0), photo0.Tags().Len())

	assert.Equal("1", photo1.Id())
	assert.Equal("_1", photo1.Title())
	// This photo was taken in Paris (by finding the lat/long of the Eiffel Tower), so its date should be interpreted according to that timezone, which is 9 hours ahead of PST (as of this moment).
	assert.Equal(int64(1323780668000-(9000*3600)), photo1.Date().MsSinceEpoch())
	assert.Equal(float32(48.8582641), photo1.Geoposition().Latitude())
	assert.Equal(float32(2.2923184), photo1.Geoposition().Longitude())
	assert.Equal(uint64(4), photo1.Sizes().Len()) // all images are different sizes
	assert.Equal(uint64(0), photo1.Tags().Len())
}
