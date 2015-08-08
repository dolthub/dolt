# Tagshow

This is a slideshow that displays photos from a noms database having a particular tag.

## Requirements

* Node.js: https://nodejs.org/download/
* You probably want to configure npm to [use a global module path that your user owns](https://docs.npmjs.com/getting-started/fixing-npm-permissions)

## Setup

* Import some photos into noms. Currently (`flickr`)[../flickr] is the only way to do this, but in principal anything that imports "Photo" structs would work (TODO: Add more importers - BUG 27 and BUG 28).

## Build

* `pushd ../server; go build; popd`
* `pushd ../tagdex; go build; popd`
* `./link.sh`
* `npm install`
* `npm run build`

## Run

* `../tagdex/tagdex -fs="$HOME/noms/chunks" -input-ref="<ref within chunkstore to look for photos within>" -output-ds="tagdex"`
* `../server/server -fs="$HOME/noms/chunks" -ds="tagdex"`
* `python -m SimpleHTTPServer 8081` (expects ../server to run on same host, port 8000)
* navigate to http://localhost:8081/?tag=

## Develop

* `npm run start`

This will start watchify which is continually building a shippable (but non minified) tagshow.js
