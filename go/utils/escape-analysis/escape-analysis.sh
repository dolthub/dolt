#!/bin/bash

go build -gcflags="-m -N -l" "$1" 2> >(grep -i "/$2")