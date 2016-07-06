// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

'use strict';

const chokidar = require('chokidar');
const commander = require('commander');
const fs = require('fs-extra');

// Start at 1 for the initial scan.
let pending = 1;

commander
    .usage('[options] <input-dir>')
    .option('-w, --watch', 'Watch input directory')
    .option('-d, --out-dir <output-dir>', 'Directory to copy files to')
    .option('-v, --verbose', 'Print copied file names')
    .parse(process.argv);

if (commander.args.length !== 1 || !commander.outDir) {
  commander.help();
}

chokidar.watch(commander.args[0])
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
  if (pending === 0 && !commander.watch) {
    process.exit(0);
  }
}

function copyFile(f) {
  pending++;
  const nn = newName(f);
  log(`${f} -> ${nn}\n`);
  fs.copy(f, nn, {clobber: true}, done);
}

function removeFile(f) {
  pending++;
  log(`${f} -> /dev/null\n`);
  fs.remove(f, done);
}

function newName(f) {
  return f.replace(new RegExp('^' + commander.args[0], 'g'), commander.outDir) + '.flow';
}

function log(s) {
  if (commander.verbose) {
    process.stdout.write(s);
  }
}
