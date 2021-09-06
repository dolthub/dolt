#!/bin/bash

LOGLEVEL=1

# Compiles "$1" (golang pkg) with escape-analysis logging
#   -m turns on logging with level=$LOGLEVEL
#   -N disables optimizations
#   -l disables inlining
go build -gcflags="-m=$LOGLEVEL -N -l" "$1" 2>&1 >/dev/null \
  | sed '/does not escape/d' \
  | sort | uniq
