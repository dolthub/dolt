.PHONY: help check
.DEFAULT_GOAL := help

SUBPKGS=cpu disk docker host internal load mem net process

help:  ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

check:  ## Check
	errcheck -ignore="Close|Run|Write" ./...
	golint ./... | egrep -v 'underscores|HttpOnly|should have comment|comment on exported|CamelCase|VM|UID'

build_test:  ## test only buildable
	GOOS=linux go test ./... | grep -v "exec format error"
	GOOS=freebsd go test ./... | grep -v "exec format error"
	CGO_ENABLED=0 GOOS=darwin go test ./... | grep -v "exec format error"
	CGO_ENABLED=1 GOOS=darwin go test ./... | grep -v "exec format error"
	GOOS=windows go test ./...| grep -v "exec format error"
