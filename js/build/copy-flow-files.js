'use strict';

const fs = require('fs-extra');
const glob = require('glob');
const path = require('path');

function exitIfError(err) {
  if (err) {
    console.error(err);  // eslint-disable-line
    process.exit(1);
  }
}

function newName(f) {
  let parts = f.split(path.sep);
  if (parts[0] !== 'src') {
    throw new Error(`Unexpected path: ${f}`);
  }
  parts[0] = 'dist';
  parts[parts.length - 1] += '.flow';
  return parts.join(path.sep);
}

glob('src/**/*.js', (err, files) => {
  exitIfError(err);
  for (const f of files) {
    fs.copy(f, newName(f), {clobber: true}, exitIfError);
  }
});
