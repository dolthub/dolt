# Splore

Splore is a general-purpose debug UI for exploring noms data.

![splore and counter](screenshot.png)

## Example

```
# Create some data
cd "$GOPATH/src/github.com/attic-labs/noms/clients/go/counter"
go build
./counter ldb:/tmp/sploretest:counter
./counter ldb:/tmp/sploretest:counter

# Build Splore
cd ../../js/splore
./build.py

# Launch Splore with noms-ui
cd ../../../cmd/noms-ui
go build
./noms-ui ../../clients/js/splore db="ldb:/tmp/sploretest"
```

Then, navigate to the URL printed by `noms-ui`, e.g. http://127.0.0.1:12345?db=xyz.

## Develop

Same as the example, but:
* `./build.py` is only necessary the first time.
* Also run `npm run start`, to continually build a non-minified (and thus debuggable) build.
