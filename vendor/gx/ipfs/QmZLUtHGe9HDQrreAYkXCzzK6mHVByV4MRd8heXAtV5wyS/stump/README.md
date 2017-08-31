# Stump
A simple log library, for when you don't really care to have super fancy logs.

Stump has four main log functions, `Log`, `VLog`, `Error` and `Fatal`.

`Log` is a basic log that always is shown.

`VLog` is only shown when `stump.Verbose` is set to true.

`Error` prints a prefix of `ERROR: ` before your log message,
the prefix is configurable by setting `stump.ErrorPrefix`.

`Fatal` is an error log that also calls `os.Exit` right afterwards.

## Installation
```
$ go get -u github.com/whyrusleeping/stump
```

## Usage

```go
import "github.com/whyrusleeping/stump"

func main() {
	stump.Log("Hello World!")

	name := GetName()
	stump.Log("My name is %s, do you like it?", name)

	err := DoThing()
	if err != nil {
		stump.Error(err)
		// or
		stump.Error("Got an error doing thing: ", err)
		// or
		stump.Error("Got error '%s' doing thing.", err)
	}

	err = DoImportantThing()
	if err != nil {
		Fatal(err)
	}
}
```

## Tips
While generally frowned upon, I like importing stump into my packages namespace like so:
```
import . "github.com/whyrusleeping/stump"
```

This allows you to call all the logging functions without the package prefix.
(eg. just `Log("hello")` instead of `stump.Log("hello")`)

## License
MIT
