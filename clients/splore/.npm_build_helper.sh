#!/bin/bash
SRC="babel-regenerator-runtime src/main.js"
OUT="out.js"

export NODE_ENV=$1
export BABEL_ENV=$1

node_modules/.bin/webpack $SRC $OUT || exit 1
