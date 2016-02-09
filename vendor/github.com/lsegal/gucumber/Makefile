default: test

deps:
	go get ./...

build: deps
	go install ./cmd/gucumber

test: build
	go test ./...
	gucumber
