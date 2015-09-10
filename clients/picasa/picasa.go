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
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2/google"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/marshal"
	"github.com/attic-labs/noms/types"
	"strconv"
)

const maxProcs = 25

var (
	albumIDFlag       = flag.String("album-id", "", "Import a specific album, identified by id")
	apiKeyFlag        = flag.String("api-key", "", "API keys for Google can be created at https://console.developers.google.com")
	apiKeySecretFlag  = flag.String("api-key-secret", "", "API keys for Google can be created at https://console.developers.google.com")
	authHTTPClient    *http.Client
	cachingHTTPClient *http.Client
	ds                *dataset.Dataset
	forceAuthFlag     = flag.Bool("force-auth", false, "Force re-authentication")
	quietFlag         = flag.Bool("quiet", false, "Don't print progress information")
	smallFlag         = flag.Bool("small", false, "Fetch lower resolution images from picasa")
	start             time.Time
)

func main() {
	flag.Usage = picasaUsage
	dsFlags := dataset.NewFlags()
	flag.Parse()
	cachingHTTPClient = util.CachingHttpClient()

	if *apiKeyFlag == "" || *apiKeySecretFlag == "" || cachingHTTPClient == nil {
		flag.Usage()
		return
	}

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	var cu types.Value
	if commit, ok := ds.MaybeHead(); ok {
		cu = commit.Value()
	}

	c, refreshToken := doAuthentication(cu)
	authHTTPClient = c

	// set start after authentication so we don't count that time
	start = time.Now()

	var nomsUser types.Value
	if *albumIDFlag != "" {
		newNomsUser, newNomsAlbum, _ := getAlbum(*albumIDFlag)
		if cu != nil {
			nomsUser = mergeInCurrentAlbums(cu, newNomsUser, newNomsAlbum)
		} else {
			nomsUser = newNomsUser
		}
		printStats(newNomsUser)
	} else {
		nomsUser = getAlbums()
		printStats(nomsUser)
	}

	nomsUser = setValueInNomsMap(nomsUser, "RefreshToken", types.NewString(refreshToken))
	_, ok := ds.Commit(nomsUser)
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}

func picasaUsage() {
	credentialSteps := `How to create Google API credentials:
  1) Go to http://console.developers.google.com/start
  2) From the "Select a project" pull down menu, choose "Create a project..."
  3) Fill in the "Project name" field (e.g. Picasa Importer)
  4) Agree to the terms and conditions and hit continue.
  5) Click on the "Select a project" pull down menu and choose "Manage all projects..."
  6) Click on the project you just created. On the new page, in the sidebar menu,
     click “APIs and auth”. In the submenu that opens up, click "Credentials".
  7) In the popup, click on the "Add credentials" pulldown and select "OAuth 2.0 client ID".
  8) Click the "Configure consent screen" button and fill in the "Product name" field.
     All other fields on this page are optional. Now click the save button.
  9) Select "Other" from the list of “Application Type” and fill in the “Name” field
     (e.g. Picasa Importer) and click the “Create” button.
     Your credentials will be displayed in a popup. Copy them to a safe place.`

	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s\n\n", credentialSteps)
}

func getAlbum(albumID string) (nomsUser types.Value, nomsAlbum types.Value, nomsPhotoList types.Value) {
	aj := AlbumJSON{}
	path := fmt.Sprintf("user/default/albumid/%s?alt=json&max-results=0", albumID)
	callPicasaAPI(authHTTPClient, path, &aj)
	u := User{ID: aj.Feed.UserID.V, Name: aj.Feed.UserName.V}
	a := Album{ID: aj.Feed.ID.V, Title: aj.Feed.Title.V, NumPhotos: aj.Feed.NumPhotos.V}
	nomsPhotoList = getPhotos(a, 1)
	nomsAlbum = marshal.Marshal(a)
	nomsAlbum = setValueInNomsMap(nomsAlbum, "Photos", nomsPhotoList)
	nomsUser = marshal.Marshal(u)
	nomsUser = setValueInNomsMap(nomsUser, "Albums", types.NewList(nomsAlbum))
	return nomsUser, nomsAlbum, nomsPhotoList
}

func getAlbums() (nomsUser types.Value) {
	aj := AlbumListJSON{}
	callPicasaAPI(authHTTPClient, "user/default?alt=json", &aj)
	user := User{ID: aj.Feed.UserID.V, Name: aj.Feed.UserName.V}

	if !*quietFlag {
		fmt.Printf("Found %d albums\n", len(aj.Feed.Entry))
	}
	var nomsAlbumList = types.NewList()
	for i, entry := range aj.Feed.Entry {
		a := Album{ID: entry.ID.V, Title: entry.Title.V, NumPhotos: entry.NumPhotos.V}
		nomsPhotoList := getPhotos(a, i)
		nomsAlbum := marshal.Marshal(a)
		nomsAlbum = setValueInNomsMap(nomsAlbum, "Photos", nomsPhotoList)
		nomsAlbumList = nomsAlbumList.Append(nomsAlbum)
	}

	nomsUser = marshal.Marshal(user)
	nomsUser = setValueInNomsMap(nomsUser, "Albums", nomsAlbumList)

	return nomsUser
}

func getPhotos(album Album, albumIndex int) (nomsPhotoList types.List) {
	if album.NumPhotos <= 0 {
		return nil
	}
	photos := make([]Photo, 0, album.NumPhotos)
	if !*quietFlag {
		fmt.Printf("Album #%d: %q contains %d photos... ", albumIndex, album.Title, album.NumPhotos)
	}
	for startIndex, foundPhotos := 0, true; album.NumPhotos > len(photos) && foundPhotos; startIndex += 1000 {
		foundPhotos = false
		aj := AlbumJSON{}
		path := fmt.Sprintf("user/default/albumid/%s?alt=json&max-results=1000", album.ID)
		if !*smallFlag {
			path = fmt.Sprintf("%s%s", path, "&imgmax=d")
		}
		if startIndex > 0 {
			path = fmt.Sprintf("%s%s%d", path, "&start-index=", startIndex)
		}
		callPicasaAPI(authHTTPClient, path, &aj)
		for _, e := range aj.Feed.Entry {
			foundPhotos = true
			tags := splitTags(e.MediaGroup.Tags.V)
			height, _ := strconv.Atoi(e.Height.V)
			width, _ := strconv.Atoi(e.Width.V)
			p := Photo{
				NomsName: "Photo",
				Height:   int(height),
				ID:       e.ID.V,
				Tags:     tags,
				Title:    e.Title.V,
				URL:      e.Content.Src,
				Width:    int(width),
			}
			photos = append(photos, p)
		}
	}

	pChan, rChan := getImageFetcher(len(photos))
	for i, p := range photos {
		pChan <- PhotoMessage{i, p}
	}
	close(pChan)

	refMessages := make([]RefMessage, 0, album.NumPhotos)
	for rm := range rChan {
		refMessages = append(refMessages, rm)
	}
	sort.Sort(ByIndex(refMessages))
	nomsPhotoList = types.NewList()
	for _, refMsg := range refMessages {
		nomsPhotoList = nomsPhotoList.Append(types.Ref{R: refMsg.Ref})
	}

	if !*quietFlag {
		fmt.Printf("fetched %d, elapsed time: %.2f secs\n", nomsPhotoList.Len(), time.Now().Sub(start).Seconds())
	}
	return nomsPhotoList
}

func getImageFetcher(numPhotos int) (pChan chan PhotoMessage, rChan chan RefMessage) {
	pChan = make(chan PhotoMessage, numPhotos)
	rChan = make(chan RefMessage)
	var wg sync.WaitGroup
	n := min(numPhotos, maxProcs)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(fid int) {
			for msg := range pChan {
				msg.Photo.Image = getImage(msg.Photo.URL)
				nomsPhoto := marshal.Marshal(msg.Photo)
				ref := types.WriteValue(nomsPhoto, ds.Store())
				rChan <- RefMessage{msg.Index, ref}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(rChan)
	}()
	return
}

func getImage(url string) *bytes.Reader {
	pr := getImageReader(url)
	defer pr.Close()
	buf, err := ioutil.ReadAll(pr)
	d.Chk.NoError(err)
	return bytes.NewReader(buf)
}

func getImageReader(url string) io.ReadCloser {
	r, err := cachingHTTPClient.Get(url)
	d.Chk.NoError(err)
	return r.Body
}

func printStats(nomsUser types.Value) {
	if !*quietFlag {
		numPhotos := uint64(0)
		nomsAlbums := getValueInNomsMap(nomsUser, "Albums").(types.List)
		for i := uint64(0); i < nomsAlbums.Len(); i++ {
			nomsAlbum := nomsAlbums.Get(i)
			nomsPhotos := getValueInNomsMap(nomsAlbum, "Photos").(types.List)
			numPhotos = numPhotos + nomsPhotos.Len()
		}

		fmt.Printf("Imported %d album(s), %d photo(s), time: %.2f\n", nomsAlbums.Len(), numPhotos, time.Now().Sub(start).Seconds())
	}
}

func mergeInCurrentAlbums(cu, newNomsUser, newNomsAlbum types.Value) (nomsUser types.Value) {
	newAlbumID := getValueInNomsMap(newNomsAlbum, "ID").(types.String).String()
	oldNomsAlbums := getValueInNomsMap(cu, "Albums").(types.List)
	newNomsAlbums := types.NewList()
	inserted := false
	for i := uint64(0); i < uint64(oldNomsAlbums.Len()); i++ {
		oldNomsAlbum := oldNomsAlbums.Get(i)
		oldAlbumID := getValueInNomsMap(oldNomsAlbum, "ID").(types.String).String()
		if newAlbumID != oldAlbumID {
			newNomsAlbums = newNomsAlbums.Append(oldNomsAlbum)
		} else {
			inserted = true
			newNomsAlbums = newNomsAlbums.Append(newNomsAlbum)
		}
	}
	if !inserted {
		newNomsAlbums = newNomsAlbums.Append(newNomsAlbum)
	}
	nomsUser = setValueInNomsMap(newNomsUser, "Albums", newNomsAlbums)
	return
}

func doAuthentication(currentUser types.Value) (c *http.Client, rt string) {
	if !*forceAuthFlag && currentUser != nil {
		rt = getValueInNomsMap(currentUser, "RefreshToken").(types.String).String()
		c = tryRefreshToken(rt)
	}
	if c == nil {
		c, rt = googleOAuth()
	}
	return c, rt
}

func tryRefreshToken(rt string) *http.Client {
	var c *http.Client

	if rt != "" {
		t := oauth2.Token{}
		conf := baseConfig("")
		ct := "application/x-www-form-urlencoded"
		body := fmt.Sprintf("client_id=%s&client_secret=%s&grant_type=refresh_token&refresh_token=%s", *apiKeyFlag, *apiKeySecretFlag, rt)
		r, err := cachingHTTPClient.Post(google.Endpoint.TokenURL, ct, strings.NewReader(body))
		d.Chk.NoError(err)
		if r.StatusCode == 200 {
			buf, err := ioutil.ReadAll(r.Body)
			d.Chk.NoError(err)
			json.Unmarshal(buf, &t)
			c = conf.Client(oauth2.NoContext, &t)
		}
	}
	return c
}

func googleOAuth() (*http.Client, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	d.Chk.NoError(err)

	redirectURL := "http://" + l.Addr().String()
	conf := baseConfig(redirectURL)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	state := fmt.Sprintf("%v", r.Uint32())
	u := conf.AuthCodeURL(state)

	// Redirect user to Google's consent page to ask for permission
	// for the scopes specified above.
	fmt.Printf("Visit the following URL to authorize access to your Picasa data: %v\n", u)
	code, returnedState := awaitOAuthResponse(l)
	d.Chk.Equal(state, returnedState, "Oauth state is not correct")

	// Handle the exchange code to initiate a transport.
	t, err := conf.Exchange(oauth2.NoContext, code)
	d.Chk.NoError(err)

	client := conf.Client(oauth2.NoContext, t)
	return client, t.RefreshToken
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
		} else if err := r.URL.Query().Get("error"); err == "access_denied" {
			fmt.Fprintln(os.Stderr, "Request for authorization was denied.")
			os.Exit(0)
		} else if err := r.URL.Query().Get("error"); err != "" {
			l.Close()
			d.Chk.Fail(err)
		}
	})}
	srv.Serve(l)

	return code, state
}

func callPicasaAPI(client *http.Client, path string, response interface{}) {
	u := "https://picasaweb.google.com/data/feed/api/" + path
	req, err := http.NewRequest("GET", u, nil)
	d.Chk.NoError(err)

	req.Header.Add("GData-Version", "2")
	resp, err := client.Do(req)
	d.Chk.NoError(err)

	msg := func() string {
		body := &bytes.Buffer{}
		_, err := io.Copy(body, resp.Body)
		d.Chk.NoError(err)
		return fmt.Sprintf("could not load %s: %d: %s", u, resp.StatusCode, body)
	}

	switch resp.StatusCode / 100 {
	case 4:
		d.Exp.Fail(msg())
	case 5:
		d.Chk.Fail(msg())
	}

	err = json.NewDecoder(resp.Body).Decode(response)
	d.Chk.NoError(err)
}

func baseConfig(redirectURL string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     *apiKeyFlag,
		ClientSecret: *apiKeySecretFlag,
		RedirectURL:  redirectURL,
		Scopes:       []string{"https://picasaweb.google.com/data"},
		Endpoint:     google.Endpoint,
	}
}

// General utility functions

func getValueInNomsMap(m types.Value, field string) types.Value {
	return m.(types.Map).Get(types.NewString(field))
}

func setValueInNomsMap(m types.Value, field string, value types.Value) types.Value {
	return m.(types.Map).Set(types.NewString(field), value)
}

func toJSON(str interface{}) string {
	v, err := json.Marshal(str)
	d.Chk.NoError(err)
	return string(v)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func splitTags(s string) map[string]bool {
	tags := map[string]bool{}
	for _, s := range strings.Split(s, ",") {
		s1 := strings.Trim(s, " ")
		if s1 != "" {
			tags[s1] = true
		}
	}
	return tags
}
