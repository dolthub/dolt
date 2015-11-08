pushd ../../../js2
npm install
npm run build
./link.sh
popd
npm link noms
