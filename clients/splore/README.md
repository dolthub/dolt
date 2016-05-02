# Splore

Splore is a general-purpose debug UI for exploring noms data.

![splore and counter](screenshot.png)

## Example

```
# Create some data
cd "$GOPATH/src/github.com/attic-labs/noms/clients/counter"
go build
./counter -ldb="/tmp/sploretest" -ds="counter"
./counter -ldb="/tmp/sploretest" -ds="counter"

# Build Splore
cd ../splore
./build.py

# Launch Splore with noms-view
cd ../../cmd/noms-view
go build
./noms-view serve ../../clients/splore store="ldb:/tmp/sploretest" &
```

Then, navigate to the URL printed by noms-view, e.g. http://127.0.0.1:12345?database=xyz.

## Develop

Same as the example, but:
* `./build.py` is only necessary the first time.
* Also run `npm run start`, to continually build a non-minified (and thus debuggable) build.
