# Explore

This is the beginnings of a simple tree view of all noms data.

## Requirements

* `<noms>/clients/server` (see instructions there)
* Node.js: https://nodejs.org/download/

## Build

* `cd <noms>/clients/explore`
* `npm install`
* `npm run build`

## Run

* `python -m SimpleHTTPServer 8080` (expects ../server to run on same host, port 8000)

## Develop

* `npm run start`

This will start watchify which is continually building a shippable (but non minified) explore.js
