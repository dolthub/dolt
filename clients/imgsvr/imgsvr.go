package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/nfnt/resize"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	port = flag.Int("port", 8001, "")
	cs   chunks.ChunkSource
)

func main() {
	flags := chunks.NewFlags()
	flag.Parse()
	cs = flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: *port})
	d.Chk.NoError(err)

	srv := &http.Server{Handler: http.HandlerFunc(handleRequest)}
	srv.Serve(l)
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		r := ref.Parse(req.URL.Query().Get("ref"))
		b := types.ReadValue(r, cs).(types.Blob)
		if b == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		img, format, err := image.Decode(b.Reader())
		d.Chk.NoError(err, "Could not decode image %s", r)

		maxw := getConstraintParam(req, "maxw", math.MaxUint16)
		maxh := getConstraintParam(req, "maxh", math.MaxUint16)

		img = resize.Thumbnail(uint(maxw), uint(maxh), img, resize.NearestNeighbor)

		switch format {
		case "gif", "jpeg", "png":
			w.Header().Set("Content-type", "image/"+format)
		default:
		}

		w.Header().Set("Content-encoding", "gzip")
		gz := gzip.NewWriter(w)
		err = png.Encode(gz, img)
		d.Chk.NoError(err)
		err = gz.Flush()
		d.Chk.NoError(err)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func getConstraintParam(r *http.Request, p string, def uint16) uint16 {
	v := r.URL.Query().Get(p)
	if v == "" {
		return def
	}

	i, err := strconv.ParseInt(v, 10, 16)
	d.Exp.NoError(err, "Invalid param %s: %s", p, v)
	return uint16(i)
}
