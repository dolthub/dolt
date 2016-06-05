// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package util

import (
	"flag"
	"net/http"
	"os/user"
	"path"

	"github.com/attic-labs/noms/go/d"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
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
