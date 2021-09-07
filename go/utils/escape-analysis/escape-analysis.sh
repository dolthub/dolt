#!/bin/bash

LOGLEVEL=1

# Compiles "$1" (golang pkg) with escape-analysis logging
#   -m turns on logging with level=$LOGLEVEL
#   -N disables optimizations
#   -l disables inlining
#
#   To configure this script as a Goland annotation generator,
#     add a File Watcher (Preferences > Tools > File Watchers).
#     See "goland-file-watcher-config.png" for config info.
#
go build -gcflags="-m=$LOGLEVEL -N -l" "$1" 2>&1 >/dev/null \
  | sed '/does not escape/d' \
  | sort | uniq
