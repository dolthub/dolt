# Splore

Splore is a general-purpose debug UI for exploring noms data.

![splore and counter](screenshot.png)

## Example

```
# Create some data
cd "$GOPATH/src/github.com/attic-labs/noms/samples/go/counter"
go build
./counter /tmp/sploretest::counter
./counter /tmp/sploretest::counter

# Build Splore
cd ../../js/splore
./build.py

# Launch Splore with noms-ui
cd ../../../cmd/noms-ui
go build
./noms-ui ../../samples/js/splore db="/tmp/sploretest"
```

Then, navigate to the URL printed by `noms-ui`, e.g. http://127.0.0.1:12345?db=xyz.

## Develop

Same as the example, but:
* `./build.py` is only necessary the first time.
* Also run `npm run start`, to continually build a non-minified (and thus debuggable) build.
