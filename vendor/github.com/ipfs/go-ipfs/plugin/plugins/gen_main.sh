#!/bin/bash

dir=${1:?first paramater with dir to work in is required}
pkg=${2:?second parameter with full name of the package is required}
main_pkg="$dir/main"

shortpkg="uniquepkgname"

mkdir -p "$main_pkg"

cat > "$main_pkg/main.go" <<EOL
package main
import (
	$shortpkg "$pkg"
)

var Plugins = $shortpkg.Plugins

func main() {
	panic("this is a plugin, build it as a plugin, this is here as for go#20312")
}
EOL
