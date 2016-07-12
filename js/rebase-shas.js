'use strict';

const readline = require('readline');
const glob = require('glob');
const fs = require('fs');

const rl = readline.createInterface({
  input: process.stdin,
});

const replacements = new Map();

let minus, plus;

rl.on('line', line => {
  {
    const m = line.match(/\-(sha1\-[a-f0-9]{40})/);
    if (m) {
      minus = m[1];
    }
  }
  {
    const m = line.match(/\+(sha1\-[a-f0-9]{40})/);
    if (m) {
      plus = m[1];
    }
  }
  if (minus && plus) {
    // console.log(minus, ' -> ', plus);
    replacements.set(minus, plus);
    minus = plus = undefined;
  }
});

rl.on('close', fixTests);

function fixFiles(err, files) {
  for (const path of files) {
    fixFile(path);
  }
}

function fixFile(path) {
  const s = fs.readFileSync(path, 'utf8');
  const s2 = s.split('\n').map(line => {
    for (const entry of replacements) {
      line = swap(line, entry[0], entry[1]);
    }
    return line;
  }).join('\n');
  fs.writeFileSync(path, s2);
}

function fixTests() {
  glob('**/*-test.js', fixFiles);
  glob('../**/*_test.go', fixFiles);
}

function swap(line, s1, s2) {
  if (line.indexOf(s1) !== -1) {
    return line.replace(s1, s2);
  }
  if (line.indexOf(s2) !== -1) {
    return line.replace(s2, s1);
  }
  return line;
}
