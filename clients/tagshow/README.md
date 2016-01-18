# Tagshow

This is a slideshow that displays photos that have a particular tag.

## Setup

* Tag some of your photos at either Flickr and/or Picasa (aka Google Photos)
* Import photos from Flickr and/or Picasa
  * To import photos from Flickr:
    * Apply for a [Flickr API key](https://www.flickr.com/services/apps/create/apply)
    * `cd $GOPATH/src/github.com/attic-labs/noms/clients/flickr`
    * `go build`
    * `./flickr -api-key="<apikey>" -api-key-secret="<apikeysecret>" -ldb="/tmp/tagshowdemo" -ds="flickr"`
  * To import photos from Picasa:
    * `cd $GOPATH/src/github.com/attic-labs/noms/clients/picasa`
    * `go build`
    * `./picasa`
    * Follow the instructions printed out to create Google API credentials
    * `./picasa -api-key="<apikey>" -api-key-secret="<apikeysecret>" -ldb="/tmp/tagshowdemo" -ds="picasa"`
* Index photos by tag
  * `cd $GOPATH/src/github.com/attic-labs/noms/clients/tagdex`
  * `go build`
  * `./tagdex -ldb="/tmp/tagshowdemo" -in="<input dataset to read (flickr or picasa)>" -out="tagdex"`

## Run

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/server
go build
./server -ldb="/tmp/tagshowdemo"

cd ../tagshow
./build.py
./node_modules/.bin/http-server
```

Then, navigate to [http://localhost:8080](http://localhost:8080).

## Develop

```
./link.sh  # only necessary first time, or if changes have happened in 'js'
npm install  # only necessary first time, or if deps have changed
NOMS_SERVER="http://localhost:8000" npm run start
```

This will start watchify which is continually building a shippable (but non minified) build
