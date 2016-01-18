# Splore

Splore is a general-purpose debug UI for exploring noms data.

![splore and counter](screenshot.png)

## Example

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/counter
go build
./counter -ldb="/tmp/sploretest" -ds="counter"
./counter -ldb="/tmp/sploretest" -ds="counter"

# Splore requires server to be running
cd ../server
go build
./server -ldb="/tmp/sploretest" &

cd ../splore
PYTHONPATH=$GOPATH/src/github.com/attic-labs/noms/tools ./build.py
./node_modules/.bin/http-server
```

Then, navigate to [http://localhost:8080](http://localhost:8080).


## Develop

* `./link.sh`  # only necessary first time, or if changes have happened in `js`
* `npm install`  # only necessary first time, or if deps have changed
* `NOMS_SERVER=http://localhost:8000 npm run start`

This will start watchify which is continually building a non-minified (and thus debuggable) build.
