#!/bin/sh

#TODO add go lint and go vet

verify_gofmt() {
    GOFMT="gofmt -s"
    cd "$(git rev-parse --show-toplevel)"
    bad_files=$($GOFMT -l . | grep -v Godeps)
    cd -
    if [[ -n $bad_files ]]; then
        echo "You have to run '$GOFMT' on these files:"
        echo "$bad_files"
        false
    else
        true
    fi
}

verify_gofmt
