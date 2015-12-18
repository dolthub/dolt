# Tagshow

This is a slideshow that displays photos from a noms database having a particular tag.

## Setup

* Import some photos into noms. Currently [`flickr`](../flickr) and [`picasa`](../picasa) are the only ways to do this, but in principal anything that imports `Photo` structs would work.

## Build

* `pushd ../server; go build; popd`
* `pushd ../tagdex; go build; popd`
* `./link.sh`
* `npm install`
* `npm run build`

## Run

* `../server/server ./server --ldb=/tmp/ldb`
* `../tagdex/tagdex -h http://localhost:8000  --in=flickr --out=tagdex`

* `python -m SimpleHTTPServer 8081` (expects ../server to run on same host, port 8000)
* navigate to http://localhost:8081/

## Develop

* `npm run start`

This will start watchify which is continually building a shippable (but non minified) tagshow.js
