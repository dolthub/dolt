package main

type AlbumMetadataJSON struct {
        Count string `json:"count"`
        Name string `json:"name"`
        ID string `json:"id"`
}

type AlbumPhotosJSON struct {
        Data []struct {
                Name string `json:"name"`
                Link string `json:"link"`
                CreatedTime string `json:"created_time"`
                ID string `json:"id"`
        } `json:"data"`
}

type AlbumListJSON struct {
        Data []struct {
                Name string `json:"name"`
                CreatedTime string `json:"created_time"`
                ID string `json:"id"`
        } `json:"data"`
}

type UserJSON struct {
        Name string `json:"name"`
        ID string `json:"id"`
}

// RefreshTokenJSON is used for fetching the refreshToken that gets stored
// in the nomsUser object
type RefreshTokenJSON struct {
        ID           string
        RefreshToken string
}
