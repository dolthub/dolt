package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSizes(t *testing.T) {
	assert := assert.New(t)

	getSizesFromJSON := func(j string) MapOfSizeToStringDef {
		e := EntryJSON{}
		assert.NoError(json.Unmarshal([]byte(j), &e))
		return getSizes(e)
	}

	assert.Equal(MapOfSizeToStringDef{
		SizeDef{Height: 128, Width: 103}:   "picasa.com/s128/photo.jpg",
		SizeDef{Height: 320, Width: 256}:   "picasa.com/s320/photo.jpg",
		SizeDef{Height: 640, Width: 512}:   "picasa.com/s640/photo.jpg",
		SizeDef{Height: 1024, Width: 820}:  "picasa.com/s1024/photo.jpg",
		SizeDef{Height: 1600, Width: 1280}: "picasa.com/s1600/photo.jpg",
		SizeDef{Height: 8000, Width: 6400}: "picasa.com/s8000/photo.jpg",
	}, getSizesFromJSON(`{
		"content": {"src": "picasa.com/bigphoto.jpg"},
		"gphoto$height": {"$t": "8000"},
		"gphoto$width": {"$t": "6400"},
		"media$group": {
			"media$thumbnail": [{
				"height": 80,
				"url": "picasa.com/s80/photo.jpg",
				"width": 64
			}]
		}
	}`))

	assert.Equal(MapOfSizeToStringDef{
		SizeDef{Height: 128, Width: 103}: "picasa.com/s128/photo.jpg",
		SizeDef{Height: 320, Width: 256}: "picasa.com/s320/photo.jpg",
		SizeDef{Height: 640, Width: 512}: "picasa.com/s640/photo.jpg",
		SizeDef{Height: 800, Width: 640}: "picasa.com/s800/photo.jpg",
	}, getSizesFromJSON(`{
		"content": {"src": "picasa.com/bigphoto.jpg"},
		"gphoto$height": {"$t": "800"},
		"gphoto$width": {"$t": "640"},
		"media$group": {
			"media$thumbnail": [{
				"height": 80,
				"url": "picasa.com/s80/photo.jpg",
				"width": 64
			}]
		}
	}`))

	assert.Equal(MapOfSizeToStringDef{
		SizeDef{Height: 103, Width: 128}:   "picasa.com/s128/photo.jpg",
		SizeDef{Height: 256, Width: 320}:   "picasa.com/s320/photo.jpg",
		SizeDef{Height: 512, Width: 640}:   "picasa.com/s640/photo.jpg",
		SizeDef{Height: 820, Width: 1024}:  "picasa.com/s1024/photo.jpg",
		SizeDef{Height: 1280, Width: 1600}: "picasa.com/s1600/photo.jpg",
		SizeDef{Height: 6400, Width: 8000}: "picasa.com/s8000/photo.jpg",
	}, getSizesFromJSON(`{
		"content": {"src": "picasa.com/bigphoto.jpg"},
		"gphoto$height": {"$t": "6400"},
		"gphoto$width": {"$t": "8000"},
		"media$group": {
			"media$thumbnail": [{
				"height": 64,
				"url": "picasa.com/s80/photo.jpg",
				"width": 80
			}]
		}
	}`))

	assert.Equal(MapOfSizeToStringDef{
		SizeDef{Height: 103, Width: 128}: "picasa.com/s128/photo.jpg",
		SizeDef{Height: 256, Width: 320}: "picasa.com/s320/photo.jpg",
		SizeDef{Height: 512, Width: 640}: "picasa.com/s640/photo.jpg",
		SizeDef{Height: 640, Width: 800}: "picasa.com/s800/photo.jpg",
	}, getSizesFromJSON(`{
		"content": {"src": "picasa.com/bigphoto.jpg"},
		"gphoto$height": {"$t": "640"},
		"gphoto$width": {"$t": "800"},
		"media$group": {
			"media$thumbnail": [{
				"height": 64,
				"url": "picasa.com/s80/photo.jpg",
				"width": 80
			}]
		}
	}`))

	assert.Equal(MapOfSizeToStringDef{
		SizeDef{Height: 128, Width: 128}:   "picasa.com/s128/photo.jpg",
		SizeDef{Height: 320, Width: 320}:   "picasa.com/s320/photo.jpg",
		SizeDef{Height: 640, Width: 640}:   "picasa.com/s640/photo.jpg",
		SizeDef{Height: 1000, Width: 1000}: "picasa.com/s1000/photo.jpg",
	}, getSizesFromJSON(`{
		"content": {"src": "picasa.com/bigphoto.jpg"},
		"gphoto$height": {"$t": "1000"},
		"gphoto$width": {"$t": "1000"},
		"media$group": {
			"media$thumbnail": [{
				"height": 80,
				"url": "picasa.com/s80/photo.jpg",
				"width": 80
			}]
		}
	}`))
}
