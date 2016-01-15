SRC="babel-regenerator-runtime src/main.js"
OUT="tagshow.js"

export NODE_ENV=$1
export BABEL_ENV=$1

node_modules/.bin/webpack --progress $SRC $OUT
