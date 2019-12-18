#!/bin/bash

set -e
set -o pipefail

bins="dolt git-dolt git-dolt-smudge"

plats='windows;386 windows;amd64 linux;386 linux;amd64 darwin;386 darwin;amd64'
for plat in $plats; do
  p=(${plat//;/ })
  goos=${p[0]}
  goarch=${p[1]}
  o="out/dolt-$goos-$goarch"
  mkdir -p "$o/bin"
  cp Godeps/LICENSES "$o/"
  for bin in $bins; do
    echo Building "$o/$bin"
    obin="$bin"
    if [ "$goos" = "windows" ]; then
      obin="$bin.exe"
    fi
    GOOS="$goos" GOARCH="$goarch" go build -o "$o/bin/$obin" "./cmd/$bin/"
  done
  tar czf "out/dolt-$goos-$goarch.tar.gz" -C out "dolt-$goos-$goarch"
done
