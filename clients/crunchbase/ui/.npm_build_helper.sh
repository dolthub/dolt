#!/bin/sh
SRC="node_modules/babel-regenerator-runtime/runtime.js src/main.js"
OUT="out.js"

export NODE_ENV=production

cp node_modules/nvd3/build/nv.d3.min.css nv.d3.css

node_modules/.bin/browserify \
    -p bundle-collapser/plugin \
    $SRC \
    | node_modules/.bin/uglifyjs -c -m > $OUT
