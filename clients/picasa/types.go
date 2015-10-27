package main

import (
	"bytes"

	"github.com/attic-labs/noms/ref"
)

// Photo is represents all the info about images that we download from picasa
// including the image itself
type Photo struct {
	NomsName string          `noms:"$name"`
	Height   int             `noms:"height"`
	ID       string          `noms:"ID"`
	Image    *bytes.Reader   `noms:"image"`
	Tags     map[string]bool `noms:"tags"`
	Title    string          `noms:"title"`
	URL      string          `noms:"URL"`
	Width    int             `noms:"width"`
}

// Album represents all the info about picassa albums including the list of
// photos it contains
type Album struct {
	ID        string
	Title     string
	NumPhotos int
}

// User represents the user who authenticated and a list of albums.
type User struct {
	ID     string
	Name   string
	Albums []Album
}

// PhotoMessage is used for communicating with Go routines that fetch photos
type PhotoMessage struct {
	Index int
	Photo Photo
}

// RefMessage is used for communicating results of photo fetch back to func main() {
// program
type RefMessage struct {
	Index int
	Ref   ref.Ref
}

// ByIndex is used for sorting RefMessages by index field
type ByIndex []RefMessage

func (slice ByIndex) Len() int {
	return len(slice)
}

func (slice ByIndex) Less(i, j int) bool {
	return slice[i].Index < slice[j].Index
}

func (slice ByIndex) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// AlbumJSON is used for unmarshalling results from picasa 'list photos on album' api
type AlbumJSON struct {
	Feed struct {
		UserName struct {
			V string `json:"$t"`
		} `json:"gphoto$nickname"`
		ID struct {
			V string `json:"$t"`
		} `json:"gphoto$ID"`
		NumPhotos struct {
			V int `json:"$t"`
		} `json:"gphoto$numphotos"`
		Title struct {
			V string `json:"$t"`
		}
		UserID struct {
			V string `json:"$t"`
		} `json:"gphoto$user"`
		Entry []struct {
			Content struct {
				Src  string
				Type string
			}
			Height struct {
				V string `json:"$t"`
			} `json:"gphoto$height"`
			ID struct {
				V string `json:"$t"`
			} `json:"gphoto$ID"`
			Size struct {
				V string `json:"$t"`
			} `json:"gphoto$size"`
			MediaGroup struct {
				Tags struct {
					V string `json:"$t"`
				} `json:"media$keywords"`
			} `json:"media$group"`
			Timestamp struct {
				V string `json:"$t"`
			} `json:"gphoto$timestamp"`
			Title struct {
				V string `json:"$t"`
			}
			Width struct {
				V string `json:"$t"`
			} `json:"gphoto$wIDth"`
		}
	}
}

// AlbumListJSON is used for unmarshalling results from picasa 'list albums' api
type AlbumListJSON struct {
	Feed struct {
		UserName struct {
			V string `json:"$t"`
		} `json:"gphoto$nickname"`
		Entry []struct {
			ID struct {
				V string `json:"$t"`
			} `json:"gphoto$ID"`
			NumPhotos struct {
				V int `json:"$t"`
			} `json:"gphoto$numphotos"`
			Title struct {
				V string `json:"$t"`
			}
		}
		UserID struct {
			V string `json:"$t"`
		} `json:"gphoto$user"`
	}
}

// RefreshTokenJSON is used for fetching the refreshToken that gets stored
// in the nomsUser object
type RefreshTokenJSON struct {
	ID           string
	RefreshToken string
}
