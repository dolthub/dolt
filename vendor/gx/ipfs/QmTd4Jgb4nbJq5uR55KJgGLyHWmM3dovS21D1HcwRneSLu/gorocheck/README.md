#gorocheck

A goroutine leak checker library


## Usage

Using gorocheck is simple, just import it, and run `CheckForLeaks()` at the end of your test!

Example:
```go
func TestManyThings(t *testing.T) {
	td := NewThingDoer()
	td.DoThingsAsync()
	err := td.WaitForThingsToBeDone()

	// ensure things were done properly
	if err != nil {
		t.Fatal(err)
	}

	// pass nil for no goroutine filter
	err = gorocheck.CheckForLeaks(nil)
	if err != nil {
		t.Fatal(err)
	}
}
```


