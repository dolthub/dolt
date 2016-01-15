SRC="babel-regenerator-runtime src/main.js"
OUT="out.js"

export NODE_ENV=$1
export BABEL_ENV=$1

if [ $1 != "production" ]; then
  export NOMS_SERVER=http://localhost:8000
  export NOMS_DATASET_ID=mlb/heatmap
fi

node_modules/.bin/webpack --progress $SRC $OUT
