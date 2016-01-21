#!/bin/bash
SRC="babel-regenerator-runtime src/main.js"
OUT="out.js"

export NODE_ENV=$1
export BABEL_ENV=$1

if [ $1 == "production" ]; then
  cp node_modules/nvd3/build/nv.d3.min.css nvd3.css
  cp node_modules/nvd3/build/nv.d3.min.js nvd3.js
  cp node_modules/d3/d3.min.js d3.js

else
  export NOMS_SERVER=http://localhost:8000
  export NOMS_DATASET_ID=crunchbase/index

  cp node_modules/nvd3/build/nv.d3.css nvd3.css
  cp node_modules/nvd3/build/nv.d3.js nvd3.js
  cp node_modules/d3/d3.js d3.js
fi

node_modules/.bin/webpack $SRC $OUT || exit 1
