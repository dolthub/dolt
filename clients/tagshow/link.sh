pushd ../../js
npm run build
./link.sh
popd
npm link noms
