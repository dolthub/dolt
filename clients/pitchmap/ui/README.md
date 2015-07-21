# pitchmap/ui

This is an (incomplete) sample app that visualizes pitching data as a heatmap.

## Requirements

* [`<noms>/clients/server`](../server)
* Node.js: https://nodejs.org/download/
* You probably want to configure npm to [use a global module path that your user owns](https://docs.npmjs.com/getting-started/fixing-npm-permissions)

## Build

* `./link.sh`
* `npm install`
* `npm run build`


## Run

* `python -m SimpleHTTPServer 8080` (expects ../server to run on same host, port 8000)

## Develop

* `npm run start`

This will start watchify which is continually building a shippable (but non minified) out.js
