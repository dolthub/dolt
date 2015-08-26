package util

import (
	"flag"
	"net/http"
	"os/user"
	"path"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/gregjones/httpcache"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/gregjones/httpcache/diskcache"
	"github.com/attic-labs/noms/d"
)

const (
	defaultDiskCacheDir = "~/noms/httpcache"
)

var (
	diskCacheDir = flag.String("http-cache-dir", defaultDiskCacheDir, "a directory to use for an http disk cache between runs")
)

func CachingHttpClient() *http.Client {
	if *diskCacheDir == "" {
		return nil
	}

	if *diskCacheDir == defaultDiskCacheDir {
		user, err := user.Current()
		d.Chk.NoError(err)
		*diskCacheDir = path.Join(user.HomeDir, "noms", "httpcache")
	}

	return httpcache.NewTransport(diskcache.New(*diskCacheDir)).Client()
}
