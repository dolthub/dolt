# go-detect-race

Check if the race detector is running.

I didnt find a variable to check quickly enough so I made this.


## Usage

```go
import (
  detectrace "github.com/jbenet/go-detect-race"
)

func main() {
  if detectrace.WithRace() {
    // running with -race
  } else {
    // running without -race
  }
}
```

## Why?

Because the race detector doesnt like massive stress tests. Example:
https://groups.google.com/forum/#!topic/golang-nuts/XDPHUt2LE70

## Why didn't you just use...

Please tell me about a better way of doing this. It wasn't
readily apparent to me, so I made this. But i would much prefer
an env var or some already existing var from the stdlib :)
