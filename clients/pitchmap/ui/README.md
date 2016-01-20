# pitchmap/ui

This is an (incomplete) sample app that visualizes pitching data as a heatmap.

## Setup

TODO: Someone fill in how to get and index the required data.

## Run

```
NOMS_SERVER=http://localhost:8000 NOMS_DATASET_ID=mlb/heatmap python build.py
./node_modules/.bin/http-server
```

Then, navigate to [http://localhost:8080](http://localhost:8080).

## Develop

* `./build.py`  # only necessary first time
* `NOMS_SERVER=http://localhost:8000 npm run start`

This will start watchify which is continually building a shippable (but non minified) out.js
