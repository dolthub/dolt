# pitchmap/ui

This is an (incomplete) sample app that visualizes pitching data as a heatmap.

## Setup

TODO: Someone fill in how to get and index the required data.

## Example

```
NOMS_SERVER=http://localhost:8000 NOMS_DATASET_ID=mlb/heatmap python build.py
python -m SimpleHTTPServer 8080`
```

Then, navigate to [http://localhost:8080](http://localhost:8080).

## Develop

* `./link.sh`  # only necessary first time, or if changes have happened in `js`
* `npm install`  # only necessary first time, or if deps have changed
* `NOMS_SERVER=http://localhost:8000 npm run start`

This will start watchify which is continually building a shippable (but non minified) out.js
