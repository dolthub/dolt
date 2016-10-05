# Photos

Photos is a web UI for the photo collections produced by photo-index.

## Example

```sh
cd $GOPATH/src/github.com/attic-labs/noms/samples/js
npm install # if necessary
noms serve db &

# Slurp Flickr data
cd flickr/slurp
npm run build # if necessary
node dist/main.js http://localhost:8000::flickr/slurp <see help>

# Find photos in Flickr data
cd ../find-photos
npm run build # if necessary
node dist/main.js http://localhost:8000::flickr/slurp http://localhost:8000::flickr/find-photos

# Index Flickr photos
cd ../../../go/photo-index
go build # if necessary
./photo-index --db http://localhost:8000 --out-ds photo-index flickr/find-photos

# View photos
cd ../../js/photos
./build.py # if necessary
node_modules/.bin/http-server &
open "http://localhost:8080/?index=http://localhost:8000::photo-index"
```
