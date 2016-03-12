package main

type ShapesJSON struct {
	Version string `xml:"version,attr"`
	Channel struct {
		Item []struct {
			ID     string `xml:"http://schemas.google.com/photos/2007 id"`
			Shapes struct {
				Shape []Shape `xml:"http://schemas.google.com/photos/2007 shape"`
			} `xml:"http://schemas.google.com/photos/2007 shapes"`
		} `xml:"item"`
	} `xml:"channel"`
}

type Shape struct {
	Type       string `xml:"type,attr"`
	Name       string `xml:"name,attr"`
	UpperLeft  string `xml:"upperLeft,attr"`
	LowerRight string `xml:"lowerRight,attr"`
}

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
		Entry []EntryJSON
	}
}

type EntryJSON struct {
	Content struct {
		Src string
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
		Thumbnails []struct {
			Height int
			URL    string
			Width  int
		} `json:"media$thumbnail"`
	} `json:"media$group"`
	Timestamp struct {
		V string `json:"$t"`
	} `json:"gphoto$timestamp"`
	Title struct {
		V string `json:"$t"`
	}
	Width struct {
		V string `json:"$t"`
	} `json:"gphoto$width"`
	Geo struct {
		Point struct {
			Pos struct {
				V string `json:"$t"`
			} `json:"gml$pos"`
		} `json:"gml$Point"`
	} `json:"georss$where"`
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
