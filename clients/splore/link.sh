#!/bin/sh

pushd ../../js
npm install
npm run build
./link.sh
popd
npm link noms
