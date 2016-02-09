package main

import (
	"flag"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/nfnt/resize"
)

var (
	port = flag.Int("port", 8001, "")
	ds   datas.DataStore
)

func main() {
	flags := datas.NewFlags()
	flag.Parse()
	ds, ok := flags.CreateDataStore()
	if !ok {
		flag.Usage()
		return
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	d.Chk.NoError(err)

	srv := &http.Server{Handler: http.HandlerFunc(handleRequest)}

	// Shutdown server gracefully so that profile may be written
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		l.Close()
		ds.Close()
	}()

	srv.Serve(l)
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		r := ref.Parse(req.URL.Query().Get("ref"))
		b := types.ReadValue(r, ds).(types.Blob)
		if b == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		img, format, err := image.Decode(b.Reader())
		d.Chk.NoError(err, "Could not decode image %s", r)

		maxw := getConstraintParam(req, "maxw", math.MaxUint16)
		maxh := getConstraintParam(req, "maxh", math.MaxUint16)

		img = resize.Thumbnail(uint(maxw), uint(maxh), img, resize.NearestNeighbor)

		w.Header().Set("Content-type", "image/"+format)

		switch format {
		case "gif":
			err = gif.Encode(w, img, nil)
		case "png":
			err = png.Encode(w, img)
		case "jpeg":
			err = jpeg.Encode(w, img, nil)
		}
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
