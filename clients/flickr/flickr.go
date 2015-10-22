package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/garyburd/go-oauth/oauth"
	img "github.com/attic-labs/noms/clients/gen/sha1_b525f9bca5e451c21dd9af564f0960045fbaa304"
	geo "github.com/attic-labs/noms/clients/gen/sha1_fb09d21d144c518467325465327d46489cff7c47"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

var (
	apiKeyFlag       = flag.String("api-key", "", "API keys for flickr can be created at https://www.flickr.com/services/apps/create/apply")
	apiKeySecretFlag = flag.String("api-key-secret", "", "API keys for flickr can be created at https://www.flickr.com/services/apps/create/apply")
	albumIdFlag      = flag.String("album-id", "", "Import a specific album, identified by id")
	ds               *dataset.Dataset
	user             User
	oauthClient      oauth.Client
	httpClient       *http.Client
)

type flickrCall struct {
	Stat string
}

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()

	httpClient = util.CachingHttpClient()

	if *apiKeyFlag == "" || *apiKeySecretFlag == "" || httpClient == nil {
		flag.Usage()
		return
	}

	oauthClient = oauth.Client{
		TemporaryCredentialRequestURI: "https://www.flickr.com/services/oauth/request_token",
		ResourceOwnerAuthorizationURI: "https://www.flickr.com/services/oauth/authorize",
		TokenRequestURI:               "https://www.flickr.com/services/oauth/access_token",
		Credentials: oauth.Credentials{
			Token:  *apiKeyFlag,
			Secret: *apiKeySecretFlag,
		},
	}

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	getUser()
	if *albumIdFlag != "" {
		album := getAlbum(*albumIdFlag)
		user = user.SetAlbums(user.Albums().Set(album.Id(), album))
	} else {
		user = user.SetAlbums(getAlbums())
	}
	commitUser()
}

func getUser() {
	if commit, ok := ds.MaybeHead(); ok {
		user = UserFromVal(commit.Value())
		if checkAuth() {
			return
		}
	} else {
		user = NewUser()
	}

	authUser()
}

func checkAuth() bool {
	response := struct {
		flickrCall
		User struct {
			Id       string `json:"id"`
			Username struct {
				Content string `json:"_content"`
			} `json:"username"`
		} `json:"user"`
	}{}

	err := callFlickrAPI("flickr.test.login", &response, nil)
	if err != nil {
		return false
	}

	user = user.SetId(response.User.Id).SetName(response.User.Username.Content)
	return true
}

func authUser() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	d.Chk.NoError(err)

	callbackURL := "http://" + l.Addr().String()
	tempCred, err := oauthClient.RequestTemporaryCredentials(nil, callbackURL, url.Values{
		"perms": []string{"read"},
	})
	// If we ever hear anything from the oauth handshake, it'll be acceptance. The user declining will mean we never get called.
	d.Chk.NoError(err)

	authUrl := oauthClient.AuthorizationURL(tempCred, nil)
	fmt.Printf("Visit the following URL to authorize access to your Flickr data: %v\n", authUrl)
	err = awaitOAuthResponse(l, tempCred)
	d.Chk.NoError(err)

	if !checkAuth() {
		d.Chk.Fail("checkAuth failed after oauth succeded")
	}
}

func getAlbum(id string) Album {
	response := struct {
		flickrCall
		Photoset struct {
			Id    string `json:"id"`
			Title struct {
				Content string `json:"_content"`
			} `json:"title"`
		} `json:"photoset"`
	}{}

	err := callFlickrAPI("flickr.photosets.getInfo", &response, &map[string]string{
		"photoset_id": id,
		"user_id":     user.Id(),
	})
	d.Chk.NoError(err)

	fmt.Printf("Photoset: %v\n", response.Photoset.Title.Content)

	photos := getAlbumPhotos(id)
	return NewAlbum().
		SetId(id).
		SetTitle(response.Photoset.Title.Content).
		SetPhotos(photos)
}

func getAlbums() MapOfStringToAlbum {
	response := struct {
		flickrCall
		Photosets struct {
			Photoset []struct {
				Id    string `json:"id"`
				Title struct {
					Content string `json:"_content"`
				} `json:"title"`
			} `json:"photoset"`
		} `json:"photosets"`
	}{}

	err := callFlickrAPI("flickr.photosets.getList", &response, nil)
	d.Chk.NoError(err)

	out := make(chan Album, len(response.Photosets.Photoset))
	for _, p := range response.Photosets.Photoset {
		p := p
		go func() {
			out <- getAlbum(p.Id)
		}()
	}

	albums := NewMapOfStringToAlbum()
	for {
		if albums.Len() == uint64(len(response.Photosets.Photoset)) {
			break
		}
		a := <-out
		albums = albums.Set(a.Id(), a)
	}

	return albums
}

func getAlbumPhotos(id string) SetOfsha1_b525f9bca5e451c21dd9af564f0960045fbaa304_RemotePhoto {
	response := struct {
		flickrCall
		Photoset struct {
			Photo []struct {
				Id             string      `json:"id"`
				Title          string      `json:"title"`
				Tags           string      `json:"tags"`
				ThumbURL       string      `json:"url_t"`
				ThumbWidth     interface{} `json:"width_t"`
				ThumbHeight    interface{} `json:"height_t"`
				SmallURL       string      `json:"url_s"`
				SmallWidth     interface{} `json:"width_s"`
				SmallHeight    interface{} `json:"height_s"`
				Latitude       interface{} `json:"latitude"`
				Longitude      interface{} `json:"longitude"`
				MediumURL      string      `json:"url_m"`
				MediumWidth    interface{} `json:"width_m"`
				MediumHeight   interface{} `json:"height_m"`
				LargeURL       string      `json:"url_l"`
				LargeWidth     interface{} `json:"width_l"`
				LargeHeight    interface{} `json:"height_l"`
				OriginalURL    string      `json:"url_o"`
				OriginalWidth  interface{} `json:"width_o"`
				OriginalHeight interface{} `json:"height_o"`
			} `json:"photo"`
		} `json:"photoset"`
	}{}

	// TODO: Implement paging. This call returns a maximum of 500 pictures in each response.
	err := callFlickrAPI("flickr.photosets.getPhotos", &response, &map[string]string{
		"photoset_id": id,
		"user_id":     user.Id(),
		"extras":      "geo,tags,url_t,url_s,url_m,url_l,url_o",
	})
	d.Chk.NoError(err)

	photos := NewSetOfsha1_b525f9bca5e451c21dd9af564f0960045fbaa304_RemotePhoto()

	for _, p := range response.Photoset.Photo {
		photo := img.RemotePhotoDef{
			Id:    p.Id,
			Title: p.Title,
			Tags:  getTags(p.Tags),
		}.New()

		sizes := img.NewMapOfSizeToString()
		sizes = addSize(sizes, p.ThumbURL, p.ThumbWidth, p.ThumbHeight)
		sizes = addSize(sizes, p.SmallURL, p.SmallWidth, p.SmallHeight)
		sizes = addSize(sizes, p.MediumURL, p.MediumWidth, p.MediumHeight)
		sizes = addSize(sizes, p.LargeURL, p.LargeWidth, p.LargeHeight)
		sizes = addSize(sizes, p.OriginalURL, p.OriginalWidth, p.OriginalHeight)
		photo = photo.SetSizes(sizes)

		lat := deFlickr(p.Latitude)
		lon := deFlickr(p.Longitude)
		if lat != 0.0 && lon != 0.0 {
			photo = photo.SetGeoposition(geo.GeopositionDef{lat, lon}.New())
		}

		photos = photos.Insert(photo)
	}

	return photos
}

func getTags(tagStr string) (tags img.SetOfStringDef) {
	tags = img.SetOfStringDef{}

	if tagStr == "" {
		return
	}

	for _, tag := range strings.Split(tagStr, " ") {
		tags[tag] = true
	}
	return
}

func deFlickr(argh interface{}) float32 {
	switch argh := argh.(type) {
	case float64:
		return float32(argh)
	case string:
		f64, err := strconv.ParseFloat(argh, 32)
		d.Chk.NoError(err)
		return float32(f64)
	default:
		return 0.0
	}
}

func addSize(sizes img.MapOfSizeToString, url string, width interface{}, height interface{}) img.MapOfSizeToString {
	getDim := func(v interface{}) uint32 {
		switch v := v.(type) {
		case float64:
			return uint32(v)
		case string:
			i, err := strconv.Atoi(v)
			d.Chk.NoError(err)
			return uint32(i)
		default:
			d.Chk.Fail(fmt.Sprintf("Unexpected value for image width or height: %+v", v))
			return uint32(0)
		}
	}
	if url == "" {
		return sizes
	}

	return sizes.Set(img.SizeDef{getDim(width), getDim(height)}.New(), url)
}

func awaitOAuthResponse(l net.Listener, tempCred *oauth.Credentials) error {
	var handlerError error

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("content-type", "text/plain")
		var cred *oauth.Credentials
		cred, _, handlerError = oauthClient.RequestToken(nil, tempCred, r.FormValue("oauth_verifier"))
		if handlerError != nil {
			fmt.Fprintf(w, "%v", handlerError)
		} else {
			fmt.Fprintf(w, "Authorized")
			user = user.SetOAuthToken(cred.Token).SetOAuthSecret(cred.Secret)
		}
		l.Close()
	})}
	srv.Serve(l)

	return handlerError
}

func commitUser() {
	ok := false
	*ds, ok = ds.Commit(user)
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}

func callFlickrAPI(method string, response interface{}, args *map[string]string) error {
	tokenCred := &oauth.Credentials{
		user.OAuthToken(),
		user.OAuthSecret(),
	}

	values := url.Values{
		"method":         []string{method},
		"format":         []string{"json"},
		"nojsoncallback": []string{"1"},
	}

	if args != nil {
		for k, v := range *args {
			values[k] = []string{v}
		}
	}

	res, err := oauthClient.Get(nil, tokenCred, "https://api.flickr.com/services/rest/", values)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	buff, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	if err = json.Unmarshal(buff, response); err != nil {
		return err
	}

	status := reflect.ValueOf(response).Elem().FieldByName("Stat").Interface().(string)
	if status != "ok" {
		err = errors.New(fmt.Sprintf("Failed flickr API call: %v, status: %v", method, status))
	}
	return nil
}
