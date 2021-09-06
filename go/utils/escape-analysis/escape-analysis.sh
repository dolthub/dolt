#!/bin/bash

go build -gcflags="-m -N -l" "$1" 2>&1 >/dev/null \
  | grep -i "/$2" \
  | sed '/does not escape/d' \
  | sort -s \
  | uniq
