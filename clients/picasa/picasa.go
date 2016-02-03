package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2"
	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/oauth2/google"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
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

type shapeMap map[string][]Shape

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
	defer ds.Store().Close()

	var currentUser *User
	if commit, ok := ds.MaybeHead(); ok {
		if currentUserRef, ok := commit.Value().(RefOfUser); ok {
			cu := currentUserRef.TargetValue(ds.Store())
			currentUser = &cu
		}
	}

	var refreshToken string
	authHTTPClient, refreshToken = doAuthentication(currentUser)

	// set start after authentication so we don't count that time
	start = time.Now()

	var user *User
	if *albumIDFlag != "" {
		newUser := getSingleAlbum(*albumIDFlag)
		if currentUser != nil {
			user = mergeInCurrentAlbums(currentUser, newUser)
		} else {
			user = newUser
		}
	} else {
		user = getAlbums()
	}

	printStats(user)

	*user = user.SetRefreshToken(refreshToken)
	userRef := types.WriteValue(*user, ds.Store())
	fmt.Printf("userRef: %s\n", userRef)
	_, err := ds.Commit(NewRefOfUser(userRef))
	d.Exp.NoError(err)
}

func picasaUsage() {
	credentialSteps := `How to create Google API credentials:
  1) Go to http://console.developers.google.com/start
  2) From the “Select a project” pull down menu, choose “Create a project...”
  3) Fill in the “Project name” field (e.g. Picasa Importer), agree to the terms
	   and conditions (if any), and hit “Create”.
	4) Wait for the “Dashboard” page to load.
	5) Click “Enable and manage APIs” in the blue “Use Google APIs” card.
	6) In the sidebar menu, click “Credentials”.
	7) Click “New credentials” and select “OAuth client ID”.
	8) Click “Configure consent screen” and fill in the “Product name” field. All
		 other fields on this page are optional. Now click “Save”.
  9) Select “Other” from the list of “Application Type”, fill in the “Name” field
     (e.g. Picasa Importer), and click “Create”.
     Your credentials will be displayed in a popup. Copy them to a safe place.`

	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s\n\n", credentialSteps)
}

func getSingleAlbum(albumID string) *User {
	aj := AlbumJSON{}
	path := fmt.Sprintf("user/default/albumid/%s?alt=json&max-results=0", albumID)
	callPicasaAPI(authHTTPClient, path, &aj)
	u := NewUser().
		SetId(aj.Feed.UserID.V).
		SetName(aj.Feed.UserName.V)

	albums := NewMapOfStringToAlbum()
	albums = getAlbum(0, aj.Feed.ID.V, aj.Feed.Title.V, uint32(aj.Feed.NumPhotos.V), albums)

	types.WriteValue(albums, ds.Store())
	u = u.SetAlbums(albums)
	return &u
}

func getAlbums() *User {
	alj := AlbumListJSON{}
	callPicasaAPI(authHTTPClient, "user/default?alt=json", &alj)
	if !*quietFlag {
		fmt.Printf("Found %d albums\n", len(alj.Feed.Entry))
	}
	albums := NewMapOfStringToAlbum()
	user := NewUser().
		SetId(alj.Feed.UserID.V).
		SetName(alj.Feed.UserName.V)
	for i, entry := range alj.Feed.Entry {
		albums = getAlbum(i, entry.ID.V, entry.Title.V, uint32(entry.NumPhotos.V), albums)
	}

	types.WriteValue(albums, ds.Store())
	user = user.SetAlbums(albums)
	return &user
}

func getShapes(albumId string) shapeMap {
	rc := callPicasaURL(authHTTPClient, fmt.Sprintf("https://picasaweb.google.com/data/feed/back_compat/user/default/albumid/%s?alt=rss&kind=photo&v=4&fd=shapes2", albumId))
	defer rc.Close()

	var shapes ShapesJSON
	err := xml.NewDecoder(rc).Decode(&shapes)
	d.Chk.NoError(err)

	res := shapeMap{}
	for _, item := range shapes.Channel.Item {
		for _, shape := range item.Shapes.Shape {
			res[item.ID] = append(res[item.ID], shape)
		}
	}

	return res
}

func getAlbum(albumIndex int, albumId, albumTitle string, numPhotos uint32, albums MapOfStringToAlbum) MapOfStringToAlbum {
	shapes := getShapes(albumId)

	a := NewAlbum().
		SetId(albumId).
		SetTitle(albumTitle)
	if numPhotos != 0 {
		remotePhotos := getRemotePhotos(&a, albumIndex, numPhotos, shapes)
		a = a.SetPhotos(remotePhotos)
	}

	return albums.Set(a.Id(), a)
}

func getRemotePhotos(album *Album, albumIndex int, numPhotos uint32, shapes shapeMap) SetOfRemotePhoto {
	remotePhotos := NewSetOfRemotePhoto()
	if !*quietFlag {
		fmt.Printf("Album #%d: %q contains %d photos... ", albumIndex, album.Title(), numPhotos)
	}
	for startIndex, foundPhotos := 0, true; uint64(numPhotos) > remotePhotos.Len() && foundPhotos; startIndex += 1000 {
		foundPhotos = false
		aj := AlbumJSON{}
		path := fmt.Sprintf("user/default/albumid/%s?alt=json&max-results=1000", album.Id())
		if !*smallFlag {
			path = fmt.Sprintf("%s%s", path, "&imgmax=d")
		}
		if startIndex > 0 {
			path = fmt.Sprintf("%s%s%d", path, "&start-index=", startIndex)
		}
		callPicasaAPI(authHTTPClient, path, &aj)
		for _, e := range aj.Feed.Entry {
			foundPhotos = true

			p := RemotePhotoDef{
				Id:          e.ID.V,
				Title:       e.Title.V,
				Geoposition: toGeopos(e.Geo.Point.Pos.V),
				Sizes:       getSizes(e),
				Tags:        splitTags(e.MediaGroup.Tags.V),
				Faces:       getFaces(e, shapes),
			}.New()

			// Timestamp is ms since the epoch.
			if i, err := strconv.ParseInt(e.Timestamp.V, 10, 64); err == nil {
				p = p.SetDate(NewDate().SetMsSinceEpoch(i))
			} else {
				fmt.Printf("Error parsing date \"%s\": %s\n", e.Timestamp.V, err)
			}

			remotePhotos = remotePhotos.Insert(p)
		}
	}

	if !*quietFlag {
		fmt.Printf("fetched %d, elapsed time: %.2f secs\n", remotePhotos.Len(), time.Now().Sub(start).Seconds())
	}
	return remotePhotos
}

func parsePoint(s string) (x, y float32) {
	pair := strings.Split(s, " ")
	return float32(atoi(pair[0])), float32(atoi(pair[1]))
}

func printStats(user *User) {
	if !*quietFlag {
		numPhotos := uint64(0)
		albums := user.Albums()
		albums.IterAll(func(id string, album Album) {
			numPhotos = numPhotos + album.Photos().Len()
		})

		fmt.Printf("Imported %d album(s), %d photo(s), time: %.2f\n", albums.Len(), numPhotos, time.Now().Sub(start).Seconds())
	}
}

func mergeInCurrentAlbums(curUser *User, newUser *User) *User {
	albums := curUser.Albums()
	newUser.Albums().IterAll(func(id string, a Album) {
		albums = albums.Set(id, a)
	})
	*newUser = newUser.SetAlbums(albums)
	return newUser
}

func doAuthentication(currentUser *User) (c *http.Client, rt string) {
	if !*forceAuthFlag && currentUser != nil {
		rt = currentUser.RefreshToken()
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
	fmt.Printf("Visit the following URL to authorize access to your Picasa data:\n%v\n", u)
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
	rc := callPicasaURL(client, "https://picasaweb.google.com/data/feed/api/"+path)
	defer rc.Close()
	err := json.NewDecoder(rc).Decode(response)
	d.Chk.NoError(err)
}

func callPicasaURL(client *http.Client, url string) io.ReadCloser {
	req, err := http.NewRequest("GET", url, nil)
	d.Chk.NoError(err)

	req.Header.Add("GData-Version", "2")
	resp, err := client.Do(req)
	d.Chk.NoError(err)

	msg := func() string {
		body := &bytes.Buffer{}
		_, err := io.Copy(body, resp.Body)
		d.Chk.NoError(err)
		return fmt.Sprintf("could not load %s: %d: %s", url, resp.StatusCode, body)
	}

	switch resp.StatusCode / 100 {
	case 4:
		d.Exp.Fail(msg())
	case 5:
		d.Chk.Fail(msg())
	}

	return resp.Body
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
func toGeopos(s string) GeopositionDef {
	s1 := strings.TrimSpace(s)
	geoPos := GeopositionDef{Latitude: 0.0, Longitude: 0.0}
	if s1 != "" {
		slice := strings.Split(s1, " ")
		lat, err := strconv.ParseFloat(slice[0], 32)
		if err == nil {
			geoPos.Latitude = float32(lat)
		}
		lon, err := strconv.ParseFloat(slice[1], 32)
		if err == nil {
			geoPos.Longitude = float32(lon)
		}
	}
	return geoPos
}

func toJSON(str interface{}) string {
	v, err := json.Marshal(str)
	d.Chk.NoError(err)
	return string(v)
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

func getSizes(e EntryJSON) MapOfSizeToStringDef {
	sizes := MapOfSizeToStringDef{}
	addSize := func(height, width int, url string) {
		sizes[SizeDef{Height: uint32(height), Width: uint32(width)}] = url
	}

	sizePath := func(size int) string {
		return fmt.Sprintf("/s%d/", size)
	}

	scale := func(x, a, b int) int {
		return int(math.Ceil(float64(x) * (float64(a) / float64(b))))
	}

	// Original size.
	height, width := atoi(e.Height.V), atoi(e.Width.V)
	addSize(height, width, e.Content.Src)

	// Infer 5 more sizes from the photo's thumbnail. Thumbnail URLs encode their size using a path component like "/s1024/" within "https://googleusercontent.com/aaa/bbb/ccc/s1024/img.jpg" and Picasa will allow any size to be specified there.
	if len(e.MediaGroup.Thumbnails) == 0 {
		return sizes
	}

	var thumbURLParts []string
	if t := e.MediaGroup.Thumbnails[0]; t.Height > t.Width {
		thumbURLParts = strings.SplitN(t.URL, sizePath(t.Height), 2)
	} else {
		thumbURLParts = strings.SplitN(t.URL, sizePath(t.Width), 2)
	}

	for _, px := range []int{128, 320, 640, 1024, 1600} {
		if px > height && px > width {
			break
		}

		thumbURL := strings.Join(thumbURLParts, sizePath(px))
		if height > width {
			addSize(px, scale(px, width, height), thumbURL)
		} else {
			addSize(scale(px, height, width), px, thumbURL)
		}
	}

	return sizes
}

func getFaces(e EntryJSON, shapes shapeMap) SetOfFaceDef {
	faces := SetOfFaceDef{}
	height, width := atoi(e.Height.V), atoi(e.Width.V)

	for _, f := range shapes[e.ID.V] {
		l, t := parsePoint(f.UpperLeft)
		r, b := parsePoint(f.LowerRight)
		faces[FaceDef{
			Top:        t / float32(height),
			Left:       l / float32(width),
			Width:      (r - l) / float32(width),
			Height:     (b - t) / float32(height),
			PersonName: f.Name,
		}] = true
	}

	return faces
}

func atoi(a string) int {
	i, err := strconv.Atoi(a)
	d.Chk.NoError(err)
	return i
}
