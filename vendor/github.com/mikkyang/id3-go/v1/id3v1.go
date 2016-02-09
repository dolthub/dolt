// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v1

import (
	v2 "github.com/mikkyang/id3-go/v2"
	"io"
	"os"
)

const (
	TagSize = 128
)

var (
	Genres = []string{
		"Blues", "Classic Rock", "Country", "Dance",
		"Disco", "Funk", "Grunge", "Hip-Hop",
		"Jazz", "Metal", "New Age", "Oldies",
		"Other", "Pop", "R&B", "Rap",
		"Reggae", "Rock", "Techno", "Industrial",
		"Alternative", "Ska", "Death Metal", "Pranks",
		"Soundtrack", "Euro-Techno", "Ambient", "Trip-Hop",
		"Vocal", "Jazz+Funk", "Fusion", "Trance",
		"Classical", "Instrumental", "Acid", "House",
		"Game", "Sound Clip", "Gospel", "Noise",
		"AlternRock", "Bass", "Soul", "Punk",
		"Space", "Meditative", "Instrumental Pop", "Instrumental Rock",
		"Ethnic", "Gothic", "Darkwave", "Techno-Industrial",
		"Electronic", "Pop-Folk", "Eurodance", "Dream",
		"Southern Rock", "Comedy", "Cult", "Gangsta",
		"Top 40", "Christian Rap", "Pop/Funk", "Jungle",
		"Native American", "Cabaret", "New Wave", "Psychadelic",
		"Rave", "Showtunes", "Trailer", "Lo-Fi",
		"Tribal", "Acid Punk", "Acid Jazz", "Polka",
		"Retro", "Musical", "Rock & Roll", "Hard Rock",
	}
)

// Tag represents an ID3v1 tag
type Tag struct {
	title, artist, album, year, comment string
	genre                               byte
	dirty                               bool
}

func ParseTag(readSeeker io.ReadSeeker) *Tag {
	readSeeker.Seek(-TagSize, os.SEEK_END)

	data := make([]byte, TagSize)
	n, err := io.ReadFull(readSeeker, data)
	if n < TagSize || err != nil || string(data[:3]) != "TAG" {
		return nil
	}

	return &Tag{
		title:   string(data[3:33]),
		artist:  string(data[33:63]),
		album:   string(data[63:93]),
		year:    string(data[93:97]),
		comment: string(data[97:127]),
		genre:   data[127],
		dirty:   false,
	}
}

func (t Tag) Dirty() bool {
	return t.dirty
}

func (t Tag) Title() string  { return t.title }
func (t Tag) Artist() string { return t.artist }
func (t Tag) Album() string  { return t.album }
func (t Tag) Year() string   { return t.year }

func (t Tag) Genre() string {
	if int(t.genre) < len(Genres) {
		return Genres[t.genre]
	}

	return ""
}

func (t Tag) Comments() []string {
	return []string{t.comment}
}

func (t *Tag) SetTitle(text string) {
	t.title = text
	t.dirty = true
}

func (t *Tag) SetArtist(text string) {
	t.artist = text
	t.dirty = true
}

func (t *Tag) SetAlbum(text string) {
	t.album = text
	t.dirty = true
}

func (t *Tag) SetYear(text string) {
	t.year = text
	t.dirty = true
}

func (t *Tag) SetGenre(text string) {
	t.genre = 255
	for i, genre := range Genres {
		if text == genre {
			t.genre = byte(i)
			break
		}
	}
	t.dirty = true
}

func (t Tag) Bytes() []byte {
	data := make([]byte, TagSize)

	copy(data[:3], []byte("TAG"))
	copy(data[3:33], []byte(t.title))
	copy(data[33:63], []byte(t.artist))
	copy(data[63:93], []byte(t.album))
	copy(data[93:97], []byte(t.year))
	copy(data[97:127], []byte(t.comment))
	data[127] = t.genre

	return data
}

func (t Tag) Size() int {
	return TagSize
}

func (t Tag) Version() string {
	return "1.0"
}

// Dummy methods to satisfy Tagger interface
func (t Tag) Padding() uint                      { return 0 }
func (t Tag) AllFrames() []v2.Framer             { return []v2.Framer{} }
func (t Tag) Frame(id string) v2.Framer          { return nil }
func (t Tag) Frames(id string) []v2.Framer       { return []v2.Framer{} }
func (t Tag) DeleteFrames(id string) []v2.Framer { return []v2.Framer{} }
func (t Tag) AddFrames(f ...v2.Framer)           {}
