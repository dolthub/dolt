all: examples

examples: deps example blockhandler/blockhandler

blockhandler/blockhandler:
	cd blockhandler && go build blockhandler.go
example:
	go build example.go

gx-bins:
	go get github.com/whyrusleeping/gx
	go get github.com/whyrusleeping/gx-go

deps: gx-bins
	gx --verbose install --global
	gx-go rewrite

clean: gx-bins
	gx-go rewrite --undo
	rm example
	rm blockhandler/blockhandler

.PHONY = clean deps gx-bins examples all
