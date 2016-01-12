package main

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
			Geo struct {
				Point struct {
					Pos struct {
						V string `json:"$t"`
					} `json:"gml$pos"`
				} `json:"gml$Point"`
			} `json:"georss$where"`
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
