package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/marshal"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2/google"
)

var (
	apiKeyFlag       = flag.String("api-key", "", "API keys for Google can be created at https://console.developers.google.com")
	apiKeySecretFlag = flag.String("api-key-secret", "", "API keys for Google can be created at https://console.developers.google.com")
	albumIdFlag      = flag.String("album-id", "", "Import a specific album, identified by id")
	ds               *dataset.Dataset
	httpClient       *http.Client
)

type Photo struct {
	NomsName string          `noms:"$name"`
	Height   string          `noms:"height"`
	Id       string          `noms:"id"`
	Image    *bytes.Reader   `noms:"image"`
	Tags     map[string]bool `noms:"tags"`
	Title    string          `noms:"title"`
	Url      string          `noms:"url"`
	Width    string          `noms:"width"`
}

type Album struct {
	Id        string
	Title     string
	NumPhotos int
	Photos    []Photo
}

type User struct {
	Id     string
	Name   string
	Albums []Album
}

type OauthToken struct {
	RefreshToken string
}

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()
	httpClient = util.CachingHttpClient()

	if *apiKeyFlag == "" || *apiKeySecretFlag == "" || httpClient == nil {
		flag.Usage()
		return
	}

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}

	var client *http.Client
	refreshToken := getRefreshToken()
	client = tryRefreshToken(refreshToken)
	if client == nil {
		client, refreshToken = googleOAuth()
	}
	var nomsUser types.Value
	if *albumIdFlag != "" {
		nomsUser, _, _ = getAlbum(client, *albumIdFlag)
	} else {
		nomsUser = getAlbums(client)
	}
	nomsUser = setValueInNomsMap(nomsUser, "RefreshToken", types.NewString(refreshToken))

	_, ok := ds.Commit(nomsUser)
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}

func getRefreshToken() string {
	oauthToken := &OauthToken{}
	if commit, ok := ds.MaybeHead(); ok {
		marshal.Unmarshal(commit.Value(), &oauthToken)
	}
	return oauthToken.RefreshToken
}

func tryRefreshToken(refreshToken string) (*http.Client) {
	var client *http.Client

	if refreshToken != "" {
		tok := oauth2.Token{}
		conf := baseConfig("")
		contentType := "application/x-www-form-urlencoded"
		body := fmt.Sprintf("client_id=%s&client_secret=%s&grant_type=refresh_token&refresh_token=%s", *apiKeyFlag, *apiKeySecretFlag, refreshToken)
		resp, err := httpClient.Post(google.Endpoint.TokenURL, contentType, strings.NewReader(body))
		d.Chk.NoError(err)
		if resp.StatusCode == 200 {
			buf, err := ioutil.ReadAll(resp.Body)
			d.Chk.NoError(err)
			json.Unmarshal(buf, &tok)
			client = conf.Client(oauth2.NoContext, &tok)
		}
	}
	return client
}

func getAlbum(client *http.Client, albumId string) (types.Value, types.Value, types.Value) {
	response := struct {
		Feed struct {
			UserName struct {
				V string `json:"$t"`
			} `json:"gphoto$nickname"`
			Id struct {
				V string `json:"$t"`
			} `json:"gphoto$id"`
			NumPhotos struct {
				V int `json:"$t"`
			} `json:"gphoto$numphotos"`
			Title struct {
				V string `json:"$t"`
			}
			UserId struct {
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
				Id struct {
					V string `json:"$t"`
				} `json:"gphoto$id"`
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
				} `json:"gphoto$width"`
			}
		}
	}{}

	path := fmt.Sprintf("user/default/albumid/%s?alt=json", albumId)
	callPicasaApi(client, path, &response)
	feed := response.Feed
	var nomsPhotos = types.NewList()
	for _, entry := range feed.Entry {
		image := getPhoto(entry.Content.Src)
		tags := map[string]bool{}
		for _, s := range strings.Split(entry.MediaGroup.Tags.V, ",") {
			s1 := strings.Trim(s, " ")
			if s1 != "" {
				tags[s1] = true
			}
		}
		photo := Photo{
			NomsName: "Photo",
			Height:   entry.Height.V,
			Id:       entry.Id.V,
			Image:    image,
			Tags:     tags,
			Title:    entry.Title.V,
			Url:      entry.Content.Src,
			Width:    entry.Width.V,
		}
		nomsPhoto := marshal.Marshal(photo)
		r := types.WriteValue(nomsPhoto, ds.Store())
		nomsPhotos = nomsPhotos.Append(types.Ref{r})
	}

	nomsAlbum := marshal.Marshal(Album{Id: feed.Id.V, Title: feed.Title.V, NumPhotos: feed.NumPhotos.V})
	nomsAlbum = setValueInNomsMap(nomsAlbum, "Photos", nomsPhotos)
	nomsUser := marshal.Marshal(User{Id: feed.UserId.V, Name: feed.UserName.V})
	nomsUser = setValueInNomsMap(nomsUser, "Albums", types.NewSet(nomsAlbum))
	return nomsUser, nomsAlbum, nomsPhotos
}

func getAlbums(client *http.Client) types.Value {
	response := struct {
		Feed struct {
				 UserName struct {
							  V string `json:"$t"`
						  } `json:"gphoto$nickname"`
				 Entry []struct {
					 Id struct {
							V string `json:"$t"`
						} `json:"gphoto$id"`
					 NumPhotos struct {
							V int `json:"$t"`
						} `json:"gphoto$numphotos"`
					 Title struct {
							V string `json:"$t"`
						}
				 }
				 UserId struct {
							  V string `json:"$t"`
						  } `json:"gphoto$user"`
			 }
	}{}

	callPicasaApi(client, "user/default?alt=json", &response)
	feed := response.Feed

	var nomsAlbums = types.NewList()
	for _, entry := range feed.Entry {
		_, _, nomsPhotos := getAlbum(client, entry.Id.V)
		nomsAlbum := marshal.Marshal(Album{Id: entry.Id.V, Title: entry.Title.V, NumPhotos: entry.NumPhotos.V})
		nomsAlbum = setValueInNomsMap(nomsAlbum, "Photos", nomsPhotos)
		nomsAlbums = nomsAlbums.Append(nomsAlbum)
	}

	nomsUser := marshal.Marshal(User{Id: feed.UserId.V, Name: feed.UserName.V})
	nomsUser = setValueInNomsMap(nomsUser, "Albums", nomsAlbums)

	return nomsUser
}

func googleOAuth() (*http.Client, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	d.Chk.NoError(err)

	redirectUrl := "http://" + l.Addr().String()
	conf := baseConfig(redirectUrl)
	rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	state := fmt.Sprintf("%v", rand1.Uint32())
	url := conf.AuthCodeURL(state)

	// Redirect user to Google's consent page to ask for permission
	// for the scopes specified above.
	fmt.Printf("Visit the following URL to authorize access to your Picasa data: %v\n", url)
	code, returnedState := awaitOAuthResponse(l)
	d.Chk.Equal(state, returnedState, "Oauth state is not correct")

	// Handle the exchange code to initiate a transport.
	tok, err := conf.Exchange(oauth2.NoContext, code)
	d.Chk.NoError(err)

	client := conf.Client(oauth2.NoContext, tok)
	return client, tok.RefreshToken
}

func awaitOAuthResponse(l net.Listener) (string, string) {
	var code, state string

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("code") != "" && r.URL.Query().Get("state") != "" {
			code = r.URL.Query().Get("code")
			state = r.URL.Query().Get("state")
			w.Header().Add("content-type", "text/plain")
			fmt.Fprintf(w, "Authorized")
			l.Close()
		} else if err := r.URL.Query().Get("error"); err != "" {
			l.Close()
			d.Chk.Fail(err)
		}
	})}
	srv.Serve(l)

	return code, state
}

func callPicasaApi(client *http.Client, path string, response interface{}) {
	url := "https://picasaweb.google.com/data/feed/api/" + path
	req, err := http.NewRequest("GET", url, nil)
	d.Chk.NoError(err)

	req.Header.Add("GData-Version", "2")
	resp, err := client.Do(req)
	d.Chk.NoError(err)

	err = json.NewDecoder(resp.Body).Decode(response)
	d.Chk.NoError(err)
}

func baseConfig(redirectUrl string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     *apiKeyFlag,
		ClientSecret: *apiKeySecretFlag,
		RedirectURL:  redirectUrl,
		Scopes: []string{ "https://picasaweb.google.com/data" },
		Endpoint: google.Endpoint,
	}
}

func getPhotoReader(url string) io.ReadCloser {
	resp, err := httpClient.Get(url)
	d.Chk.NoError(err)
	return resp.Body
}

func getPhoto(url string) *bytes.Reader {
	photoReader := getPhotoReader(url)
	defer photoReader.Close()
	buf, err := ioutil.ReadAll(photoReader)
	d.Chk.NoError(err)
	return bytes.NewReader(buf)
}

// General utility functions

func getValueInNomsMap(m types.Value, field string) types.Value {
	return m.(types.Map).Get(types.NewString(field))
}

func setValueInNomsMap(m types.Value, field string, value types.Value) types.Value {
	return m.(types.Map).Set(types.NewString(field), value)
}

func toJson(str interface{}) string {
	v, err := json.Marshal(str)
	d.Chk.NoError(err)
	return string(v)
}
