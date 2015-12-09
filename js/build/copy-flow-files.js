'use strict';

const chokidar = require('chokidar');
const fs = require('fs-extra');
const path = require('path');

// Start at 1 for the initial scan.
let pending = 1;

const shouldWatch = process.argv.indexOf('-w') !== -1 ||
                    process.argv.indexOf('--watch') !== -1;

chokidar.watch('./src')
    .on('add', copyFile)
    .on('change', copyFile)
    .on('unlink', removeFile)
    .on('ready', done);

function done(err) {
  pending--;
  if (err) {
    console.error(err);  // eslint-disable-line
    process.exit(1);
  }
  if (pending === 0 && !shouldWatch) {
    process.exit(0);
  }
}

function copyFile(f) {
  pending++;
  let nn = newName(f)
  process.stdout.write(`${f} -> ${nn}\n`);
  fs.copy(f, nn, {clobber: true}, done);
}

function removeFile(f) {
  pending++;
  process.stdout.write(`${f} -> /dev/null\n`);
  fs.remove(f, done);
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
