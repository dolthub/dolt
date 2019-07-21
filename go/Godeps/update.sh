#!/bin/bash

set -eo pipefail

go list -deps -json ./cmd/dolt/. ./cmd/git-dolt/. ./cmd/git-dolt-smudge/. | go run ./utils/3pdeps/. > ./Godeps/LICENSES
