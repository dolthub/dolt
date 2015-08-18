package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/garyburd/go-oauth/oauth"
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

	getUser()
	if *albumIdFlag != "" {
		getAlbum(*albumIdFlag)
	} else {
		getAlbums()
	}
	commitUser()
}

func getUser() {
	commits := ds.Heads()
	if commits.Len() > uint64(0) {
		user = UserFromVal(commits.Any().Value())
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

	user = user.SetId(types.NewString(response.User.Id)).SetName(types.NewString(response.User.Username.Content))
	return true
}

func authUser() {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	d.Chk.NoError(err)

	callbackURL := "http://" + l.Addr().String()
	tempCred, err := oauthClient.RequestTemporaryCredentials(nil, callbackURL, url.Values{
		"perms": []string{"read"},
	})
	// If we ever hear anything from the oauth handshake, it'll be acceptance. The user declining will mean we never get called.
	d.Chk.NoError(err)

	authUrl := oauthClient.AuthorizationURL(tempCred, nil)
	fmt.Printf("Go to the following URL to authorize: %v\n", authUrl)
	err = awaitOAuthResponse(l, tempCred)
	d.Chk.NoError(err)

	if !checkAuth() {
		d.Chk.Fail("checkAuth failed after oauth succeded")
	}
}

func getAlbum(id string) {
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
		"user_id":     user.Id().String(),
	})
	d.Chk.NoError(err)

	fmt.Printf("\nPhotoset: %v\n", response.Photoset.Title)

	// TODO: Retrieving a field which hasn't been set will crash, so we have to reach inside and test the untyped
	var albums MapOfStringToAlbum
	if !user.NomsValue().Has(types.NewString("albums")) {
		albums = NewMapOfStringToAlbum()
	} else {
		albums = user.Albums()
	}

	photos := getAlbumPhotos(id)
	album := NewAlbum().
		SetId(types.NewString(id)).
		SetTitle(types.NewString(response.Photoset.Title.Content)).
		SetPhotos(photos)
	albums = albums.Set(types.NewString(id), album)
	user = user.SetAlbums(albums)
}

func getAlbums() {
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

	for _, p := range response.Photosets.Photoset {
		getAlbum(p.Id)
	}
}

func getAlbumPhotos(id string) SetOfPhoto {
	response := struct {
		flickrCall
		Photoset struct {
			Photo []struct {
				Id    string `json:"id"`
				Title string `json:"title"`
				Tags  string `json:"tags"`
			} `json:"photo"`
		} `json:"photoset"`
	}{}

	// TODO: Implement paging. This call returns a maximum of 500 pictures in each response.
	err := callFlickrAPI("flickr.photosets.getPhotos", &response, &map[string]string{
		"photoset_id": id,
		"user_id":     user.Id().String(),
		"extras":      "tags",
	})
	d.Chk.NoError(err)

	photos := types.NewSet()
	for _, p := range response.Photoset.Photo {
		url := getOriginalUrl(p.Id)
		fmt.Printf(" . %v\n", url)
		photoReader := getPhotoReader(url)
		defer photoReader.Close()
		b, err := types.NewBlob(photoReader)
		d.Chk.NoError(err)
		photo := NewPhoto().
			SetId(types.NewString(p.Id)).
			SetTitle(types.NewString(p.Title)).
			SetUrl(types.NewString(url)).
			SetTags(getTags(p.Tags)).
			SetImage(b)
		// The photo is big, so write it out now to release the memory.
		r := types.WriteValue(photo.NomsValue(), ds)
		photos = photos.Insert(types.Ref{r})
	}
	return SetOfPhotoFromVal(photos)
}

func getTags(tagStr string) (res SetOfString) {
	res = NewSetOfString()
	if tagStr == "" {
		return
	}
	for _, tag := range strings.Split(tagStr, " ") {
		res = res.Insert(types.NewString(tag))
	}
	return res
}

func getOriginalUrl(id string) string {
	response := struct {
		flickrCall
		Sizes struct {
			Size []struct {
				Label  string `json:"label"`
				Source string `json:"source"`
				// TODO: For some reason json unmarshalling was getting confused about types. Not sure why.
				// Width  int `json:"width"`
				// Height int `json:"height"`
			} `json:"size"`
		} `json:"sizes"`
	}{}

	err := callFlickrAPI("flickr.photos.getSizes", &response, &map[string]string{
		"photo_id": id,
	})
	d.Chk.NoError(err)

	for _, p := range response.Sizes.Size {
		if p.Label == "Original" {
			return p.Source
		}
	}
	d.Chk.Fail(fmt.Sprintf("No Original image size found photo: %v", id))
	return "NOT REACHED"
}

func getPhotoReader(url string) io.ReadCloser {
	resp, err := httpClient.Get(url)
	d.Chk.NoError(err)
	return resp.Body
}

func awaitOAuthResponse(l *net.TCPListener, tempCred *oauth.Credentials) error {
	var handlerError error

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("content-type", "text/plain")
		var cred *oauth.Credentials
		cred, _, handlerError = oauthClient.RequestToken(nil, tempCred, r.FormValue("oauth_verifier"))
		if handlerError != nil {
			fmt.Fprintf(w, "%v", handlerError)
		} else {
			fmt.Fprintf(w, "Authorized")
			user = user.SetOAuthToken(types.NewString(cred.Token)).SetOAuthSecret(types.NewString(cred.Secret))
		}
		l.Close()
	})}
	srv.Serve(l)

	return handlerError
}

func commitUser() {
	commits := ds.Heads()
	commitSet := datas.NewSetOfCommit().Insert(
		datas.NewCommit().SetParents(
			commits.NomsValue()).SetValue(
			user.NomsValue()))
	ds.Commit(commitSet)
}

func callFlickrAPI(method string, response interface{}, args *map[string]string) error {
	tokenCred := &oauth.Credentials{
		user.OAuthToken().String(),
		user.OAuthSecret().String(),
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
