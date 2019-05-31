#!/bin/sh

# Print out a psuedo-version for the current HEAD, acceptable for use in go.mod.
# Used in a replace, for example:
#
# replace github.com/attic-labs/noms => github.com/liquidata-inc/noms v0.0.0-20190531204628-499e9652fee4

TZ=UTC exec git show --quiet --date='format-local:%Y%m%d%H%M%S' --format="v0.0.0-%cd-%H" | sed 's/.\{28\}$//'
