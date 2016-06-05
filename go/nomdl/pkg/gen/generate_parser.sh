#! /bin/sh

"$GOPATH/bin/pigeon" "${1}" | "$GOPATH/bin/goimports" > "${1}".go
