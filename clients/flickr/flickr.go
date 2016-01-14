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
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/bradfitz/latlong"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/garyburd/go-oauth/oauth"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
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

type flickrAPI interface {
	Call(method string, response interface{}, args *map[string]string) error
}

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

	api := liveFlickrAPI{}
	getUser(api)
	if *albumIdFlag != "" {
		album := getAlbum(api, *albumIdFlag)
		user = user.SetAlbums(user.Albums().Set(album.Id(), album))
	} else {
		user = user.SetAlbums(getAlbums(api))
	}
	commitUser()
}

func getUser(api flickrAPI) {
	if commit, ok := ds.MaybeHead(); ok {
		userRef := commit.Value().(RefOfUser)
		user = userRef.TargetValue(ds.Store())
		if checkAuth(api) {
			return
		}
	} else {
		user = NewUser(ds.Store())
	}

	authUser(api)
}

func checkAuth(api flickrAPI) bool {
	response := struct {
		flickrCall
		User struct {
			Id       string `json:"id"`
			Username struct {
				Content string `json:"_content"`
			} `json:"username"`
		} `json:"user"`
	}{}

	err := api.Call("flickr.test.login", &response, nil)
	if err != nil {
		return false
	}

	user = user.SetId(response.User.Id).SetName(response.User.Username.Content)
	return true
}

func authUser(api flickrAPI) {
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

	if !checkAuth(api) {
		d.Chk.Fail("checkAuth failed after oauth succeded")
	}
}

func getAlbum(api flickrAPI, id string) Album {
	response := struct {
		flickrCall
		Photoset struct {
			Id    string `json:"id"`
			Title struct {
				Content string `json:"_content"`
			} `json:"title"`
		} `json:"photoset"`
	}{}

	err := api.Call("flickr.photosets.getInfo", &response, &map[string]string{
		"photoset_id": id,
		"user_id":     user.Id(),
	})
	d.Chk.NoError(err)

	photos := getAlbumPhotos(api, id)

	fmt.Printf("Photoset: %v\nRef: %s\n", response.Photoset.Title.Content, photos.TargetRef())

	return NewAlbum(ds.Store()).
		SetId(id).
		SetTitle(response.Photoset.Title.Content).
		SetPhotos(photos)
}

func getAlbums(api flickrAPI) MapOfStringToAlbum {
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

	err := api.Call("flickr.photosets.getList", &response, nil)
	d.Chk.NoError(err)

	out := make(chan Album, len(response.Photosets.Photoset))
	for _, p := range response.Photosets.Photoset {
		p := p
		go func() {
			out <- getAlbum(api, p.Id)
		}()
	}

	albums := NewMapOfStringToAlbum(ds.Store())
	for {
		if albums.Len() == uint64(len(response.Photosets.Photoset)) {
			break
		}
		a := <-out
		albums = albums.Set(a.Id(), a)
	}

	return albums
}

func getAlbumPhotos(api flickrAPI, id string) RefOfSetOfRefOfRemotePhoto {
	response := struct {
		flickrCall
		Photoset struct {
			Photo []struct {
				DateTaken      string      `json:"datetaken"`
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
	err := api.Call("flickr.photosets.getPhotos", &response, &map[string]string{
		"photoset_id": id,
		"user_id":     user.Id(),
		"extras":      "date_taken,geo,tags,url_t,url_s,url_m,url_l,url_o",
	})
	d.Chk.NoError(err)

	cs := ds.Store()
	photos := NewSetOfRefOfRemotePhoto(cs)

	for _, p := range response.Photoset.Photo {
		photo := RemotePhotoDef{
			Id:    p.Id,
			Title: p.Title,
			Tags:  getTags(p.Tags),
		}.New(cs)

		lat, lon := deFlickr(p.Latitude), deFlickr(p.Longitude)

		// Flickr doesn't give timezone information (in fairness, neither does EXIF), so try to figure it out from the geolocation data. This is imperfect because it won't give us daylight savings. If there is no geolocation data then assume the location is PST - it's better than GMT.
		zone := "America/Los_Angeles"
		if lat != 0.0 && lon != 0.0 {
			if z := latlong.LookupZoneName(lat, lon); z != "" {
				zone = z
			}
		}
		location, err := time.LoadLocation(zone)
		d.Chk.NoError(err)

		// DateTaken is the MySQL DATETIME format.
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", p.DateTaken, location); err == nil {
			photo = photo.SetDate(DateDef{t.Unix()}.New(cs))
		} else {
			fmt.Printf("Error parsing date \"%s\": %s\n", p.DateTaken, err)
		}

		sizes := NewMapOfSizeToString(cs)
		sizes = addSize(sizes, p.ThumbURL, p.ThumbWidth, p.ThumbHeight)
		sizes = addSize(sizes, p.SmallURL, p.SmallWidth, p.SmallHeight)
		sizes = addSize(sizes, p.MediumURL, p.MediumWidth, p.MediumHeight)
		sizes = addSize(sizes, p.LargeURL, p.LargeWidth, p.LargeHeight)
		sizes = addSize(sizes, p.OriginalURL, p.OriginalWidth, p.OriginalHeight)
		photo = photo.SetSizes(sizes)

		if lat != 0.0 && lon != 0.0 {
			photo = photo.SetGeoposition(GeopositionDef{float32(lat), float32(lon)}.New(cs))
		}

		photos = photos.Insert(NewRefOfRemotePhoto(types.WriteValue(photo, cs)))
	}

	r := types.WriteValue(photos, cs)
	return NewRefOfSetOfRefOfRemotePhoto(r)
}

func getTags(tagStr string) (tags SetOfStringDef) {
	tags = SetOfStringDef{}

	if tagStr == "" {
		return
	}

	for _, tag := range strings.Split(tagStr, " ") {
		tags[tag] = true
	}
	return
}

func deFlickr(argh interface{}) float64 {
	switch argh := argh.(type) {
	case float64:
		return argh
	case string:
		f64, err := strconv.ParseFloat(argh, 64)
		d.Chk.NoError(err)
		return float64(f64)
	default:
		return 0.0
	}
}

func addSize(sizes MapOfSizeToString, url string, width interface{}, height interface{}) MapOfSizeToString {
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

	return sizes.Set(SizeDef{getDim(width), getDim(height)}.New(ds.Store()), url)
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
	var err error
	r := NewRefOfUser(types.WriteValue(user, ds.Store()))
	*ds, err = ds.Commit(r)
	d.Exp.NoError(err)
}

type liveFlickrAPI struct{}

func (api liveFlickrAPI) Call(method string, response interface{}, args *map[string]string) error {
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
