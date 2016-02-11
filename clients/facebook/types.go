package main

type UserJSON struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type PhotoListJSON struct {
	Data []struct {
		Id          string      `json:"id"`
		Name        string      `json:"name"`
		CreatedTime int         `json:"created_time"`
		Images      []ImageJSON `json:"images"`
		Tags        TagListJSON `json:"tags"`
		Place       PlaceJSON   `json:"place"`
	}
	Paging struct {
		Next string `json:"next"`
	} `json:"paging"`
}

type ImageJSON struct {
	Height uint32 `json:"height"`
	Width  uint32 `json:"width"`
	Source string `json:"source"`
}

type TagListJSON struct {
	Data []TagJSON
	// Paging, but I doubt any tag lists will be that long
}

type TagJSON struct {
	Name string  `json:"name"`
	X    float32 `json:"x"`
	Y    float32 `json:"y"`
}

type PlaceJSON struct {
	Location struct {
		Latitude  float32 `json:"latitude"`
		Longitude float32 `json:"longitude"`
	} `json:"location"`
	// The reverse-geocoded information about this place is also included, but we don't care about that now
}
