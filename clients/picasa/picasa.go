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
	apiKeyFlag       = flag.String("api-key", "616896568902-1lau1f46nbvi0miree13bgjtvarl6dp7.apps.googleusercontent.com", "API keys for google can be created at https://console.developers.google.com")
	apiKeySecretFlag = flag.String("api-key-secret", "OTnF6N3eH-ZXVmw4mZWeL2sh", "API keys for flickr can be created at https://console.developers.google.com")
	albumIdFlag      = flag.String("album-id", "", "Import a specific album, identified by id")
	ds               *dataset.Dataset
	httpClient       *http.Client
)

func googleOAuth() *http.Client {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2929})
	redirectURL := "http://" + l.Addr().String()

	conf := &oauth2.Config{
		ClientID:     *apiKeyFlag,
		ClientSecret: *apiKeySecretFlag,
		RedirectURL:  redirectURL,
		Scopes: []string{
			"https://picasaweb.google.com/data",
		},
		Endpoint: google.Endpoint,
	}
	// Redirect user to Google's consent page to ask for permission
	// for the scopes specified above.
	rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	state := fmt.Sprintf("%v", rand1.Uint32())
	url := conf.AuthCodeURL(state)
	fmt.Printf("Visit the URL for the auth dialog: %v\n", url)
	code, returnedState := awaitOAuthResponse(l)
	d.Chk.True(state == returnedState, "Oauth state is not correct")

	// Handle the exchange code to initiate a transport.
	tok, err := conf.Exchange(oauth2.NoContext, code)
	d.Chk.NoError(err)

	client := conf.Client(oauth2.NoContext, tok)
	return client
}

func awaitOAuthResponse(l *net.TCPListener) (string, string) {
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
	req, e1 := http.NewRequest("GET", url, nil)
	d.Chk.NoError(e1)

	req.Header.Add("GData-Version", "2")
	resp, e2 := client.Do(req)
	d.Chk.NoError(e2)

	defer resp.Body.Close()
	buf, e3 := ioutil.ReadAll(resp.Body)
	d.Chk.NoError(e3)
	
	e4 := json.Unmarshal(buf, response);
	d.Chk.NoError(e4)
}

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

func setValueInNomsMap(m types.Value, field string, value types.Value) types.Value {
	return m.(types.Map).Set(types.NewString(field), value)
}

func getValueInNomsMap(m types.Value, field string) types.Value {
	return m.(types.Map).Get(types.NewString(field))
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
	var nomsPhotos = types.NewSet()
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
		nomsPhotos = nomsPhotos.Insert(types.Ref{r})
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

	//	var albums = make([]Album, len(response.Feed.Entry))
	var nomsAlbums = types.NewSet()
	for _, entry := range feed.Entry {
		_, _, nomsPhotos := getAlbum(client, entry.Id.V)
		nomsAlbum := marshal.Marshal(Album{Id: entry.Id.V, Title: entry.Title.V, NumPhotos: entry.NumPhotos.V})
		nomsAlbum = setValueInNomsMap(nomsAlbum, "Photos", nomsPhotos)
		nomsAlbums = nomsAlbums.Insert(nomsAlbum)
	}

	nomsUser := marshal.Marshal(User{Id: feed.UserId.V, Name: feed.UserName.V})
	nomsUser = setValueInNomsMap(nomsUser, "Albums", nomsAlbums)

	return nomsUser
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

	client := googleOAuth()
	var nomsUser types.Value
	if *albumIdFlag != "" {
		nomsUser, _, _ = getAlbum(client, *albumIdFlag)
	} else {
		nomsUser = getAlbums(client)
	}

	_, ok := ds.Commit(nomsUser)
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}
