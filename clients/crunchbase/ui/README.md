# Crunchbase UI

This is a landing page and visualization for the crunchbase dataset.

## Setup

Before you can run the UI, you must import and index the dataset it relies on:

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/crunchbase/importer
go build
./importer -ldb="/tmp/crunchbasedemo" -ds="crunchbase/raw" "<url-to-crunchbase-spreadsheet>"

cd ../index
go build
./index -ldb="/tmp/crunchbasedemo" -in="crunchbase/raw" -out="crunchbase/index"
```

## Run

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/server
go build
./server -ldb="/tmp/crunchbasedemo"

cd ../ui
./build.py
./node_modules/.bin/http-server
```

Then, navigate to [http://localhost:8080](http://localhost:8080).


## Develop

```
./link.sh  # only necessary first time, or if changes have happened in 'js'
npm install  # only necessary first time, or if deps have changed
NOMS_SERVER="http://localhost:8000" NOMS_DATASET_ID="crunchbase/index" npm run start
```

This will start watchify which is continually building a non-minified (and thus debuggable) build.
