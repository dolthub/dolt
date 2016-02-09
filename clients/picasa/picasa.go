package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"golang.org/x/oauth2"
)

const maxProcs = 25

var (
	albumIDFlag       = flag.String("album-id", "", "Import a specific album, identified by id")
	authHTTPClient    *http.Client
	cachingHTTPClient *http.Client
	ds                *dataset.Dataset
	tokenFlag         = flag.String("token", "", "OAuth2 bearer token to authenticate with (required)")
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

	if *tokenFlag == "" || cachingHTTPClient == nil {
		flag.Usage()
		os.Exit(1)
	}

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		os.Exit(1)
	}
	defer ds.Store().Close()

	var currentUser *User
	if commit, ok := ds.MaybeHead(); ok {
		if currentUserRef, ok := commit.Value().(RefOfUser); ok {
			cu := currentUserRef.TargetValue(ds.Store())
			currentUser = &cu
		}
	}

	token := oauth2.Token{AccessToken: *tokenFlag}
	authHTTPClient = oauth2.NewClient(oauth2.NoContext, oauth2.StaticTokenSource(&token))

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

	userRef := types.WriteValue(*user, ds.Store())
	fmt.Printf("userRef: %s\n", userRef)
	_, err := ds.Commit(NewRefOfUser(userRef))
	d.Exp.NoError(err)
}

func picasaUsage() {
	essay := `You must provide -token, an OAuth2 bearer token. It will look like ab12.adsDshDsjkkdljkddhASDhjksdSAs-asjASDhADSs-asdhjdAs-SDSADhlDSAhlsjsAs. To get one:
  1) Go to https://developers.google.com/oauthplayground.
  2) Enter https://picasaweb.google.com/data in the "Input your own scopes" box, click "Authorize APIs".
  3) Click "Allow".
  4) Click "Exchange authorization code for tokens".
  5) Copy the "Access token" field (e.g. ab12.adsDshDsjkkdljkddhASDhjksdSAs-asjASDhADSs-asdhjdAs-SDSADhlDSAhlsjsAs).`
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s\n\n", essay)
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
