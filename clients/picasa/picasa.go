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
	"sync"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"golang.org/x/oauth2"
)

var (
	authHTTPClient    *http.Client
	cachingHTTPClient *http.Client
	ds                *dataset.Dataset
	tokenFlag         = flag.String("token", "", "OAuth2 bearer token to authenticate with (required)")
	progressFileFlag  = flag.String("progress-file", "", "file for logging progress")
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

	util.MaybeStartCPUProfile()
	defer util.StopCPUProfile()

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		os.Exit(1)
	}
	defer ds.Store().Close()

	token := oauth2.Token{AccessToken: *tokenFlag}
	authHTTPClient = oauth2.NewClient(oauth2.NoContext, oauth2.StaticTokenSource(&token))

	start = time.Now()
	user := getUser()
	printStats(user)

	userRef := types.WriteValue(user, ds.Store())
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

func getUser() User {
	alj := AlbumListJSON{}
	callPicasaAPI(authHTTPClient, "user/default?alt=json", &alj)
	fmt.Printf("Found %d albums\n", len(alj.Feed.Entry))
	albums := ListOfRefOfAlbumDef{}
	user := NewUser().
		SetId(alj.Feed.UserID.V).
		SetName(alj.Feed.UserName.V)

	ch := make(chan Album, len(alj.Feed.Entry))
	lim := make(chan struct{}, 25)
	wg := sync.WaitGroup{}
	wg.Add(len(alj.Feed.Entry))
	for i, entry := range alj.Feed.Entry {
		i := i
		entry := entry
		lim <- struct{}{}
		go func() {
			ch <- getAlbum(i, entry.ID.V, entry.Title.V, entry.NumPhotos.V)
			<-lim
		}()
	}

	go func() {
		for {
			album := <-ch
			r := types.WriteValue(album, ds.Store())
			albums = append(albums, r)
			wg.Done()
		}
	}()

	wg.Wait()

	return user.SetAlbums(albums.New())
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

func getAlbum(albumIndex int, albumId, albumTitle string, numPhotos int) Album {
	shapes := getShapes(albumId)
	a := NewAlbum().
		SetId(albumId).
		SetTitle(albumTitle)
	if numPhotos != 0 {
		fmt.Printf("Album #%d: %q contains %d photos...\n", albumIndex, a.Title(), numPhotos)
		remotePhotos := getRemotePhotos(albumId, numPhotos, shapes)
		a = a.SetPhotos(remotePhotos)
	}
	return a
}

func getRemotePhotos(albumId string, numPhotos int, shapes shapeMap) SetOfRefOfRemotePhoto {
	mu := sync.Mutex{}
	remotePhotos := SetOfRefOfRemotePhotoDef{}

	batchSize := 1000
	batches := int(math.Ceil(float64(numPhotos) / float64(batchSize)))
	wg := sync.WaitGroup{}
	wg.Add(batches)

	for i := 0; i < batches; i++ {
		i := i
		go func() {
			defer wg.Done()

			startIndex := i * batchSize
			aj := AlbumJSON{}
			path := fmt.Sprintf("user/default/albumid/%s?alt=json&max-results=1000", albumId)
			if startIndex > 0 {
				path = fmt.Sprintf("%s%s%d", path, "&start-index=", startIndex)
			}
			callPicasaAPI(authHTTPClient, path, &aj)
			for _, e := range aj.Feed.Entry {
				p := RemotePhotoDef{
					Id:          e.ID.V,
					Title:       e.Title.V,
					Geoposition: toGeopos(e.Geo.Point.Pos.V),
					Sizes:       getSizes(e),
					Tags:        splitTags(e.MediaGroup.Tags.V),
					Faces:       getFaces(e, shapes),
				}.New()

				// Timestamp is ms since the epoch.
				if ts, err := strconv.ParseInt(e.Timestamp.V, 10, 64); err == nil {
					p = p.SetDate(NewDate().SetMsSinceEpoch(ts))
				} else {
					fmt.Printf("Error parsing date \"%s\": %s\n", e.Timestamp.V, err)
				}

				mu.Lock()
				remotePhotos[types.WriteValue(p, ds.Store())] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return remotePhotos.New()
}

func parsePoint(s string) (x, y float32) {
	pair := strings.Split(s, " ")
	return float32(atoi(pair[0])), float32(atoi(pair[1]))
}

func printStats(user User) {
	numPhotos := uint64(0)
	albums := user.Albums()
	albums.IterAll(func(album RefOfAlbum, _ uint64) {
		// TODO: Sucks to deref album just to get num photos
		numPhotos = numPhotos + album.TargetValue(ds.Store()).Photos().Len()
	})

	fmt.Printf("Imported %d album(s), %d photo(s), time: %.2f\n", albums.Len(), numPhotos, time.Now().Sub(start).Seconds())
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
